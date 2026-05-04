# tasks/local_organize.py
import logging
import os
import re
import json
import time
import shutil
import threading
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Tuple, List, Dict

import constants
import config_manager
from database import settings_db
from database.connection import get_db_connection

logger = logging.getLogger(__name__)

VIDEO_EXTENSIONS = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'strm', 'mpg'}

_MONITOR_OBSERVER = None
_MONITOR_HANDLER = None

def get_config():
    return config_manager.APP_CONFIG

def _parse_video_filename(filename: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """从文件名解析季号和集数"""
    season = None
    episode = None

    match = re.search(r'[sS](\d{1,4})[eE](\d{1,4})', filename)
    if match:
        season = int(match.group(1))
        episode = int(match.group(2))
        return season, episode, "tv"

    match = re.search(r'(?:ep|episode)[.\-_]*(\d{1,4})', filename, re.IGNORECASE)
    if match:
        episode = int(match.group(1))
        return season, episode, "tv"

    match = re.search(r'第(\d+)季', filename)
    if match:
        season = int(match.group(1))
        return season, None, "tv"

    match = re.search(r'第(\d+)集', filename)
    if match:
        episode = int(match.group(1))
        return season, episode, "tv"

    return None, None, "movie"

def _extract_tmdb_id(path: str) -> Optional[str]:
    """从路径中提取 TMDb ID"""
    match = re.search(r'(?:tmdb|tmdbid)[=_~-]*(\d+)', path, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def _identify_media(file_path: str, source_type: str, folder_name: str = None) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    识别媒体 TMDb ID 和类型
    source_type: 'movie', 'tv', 'mixed'
    """
    from handler import tmdb as tmdb_handler

    file_name = os.path.basename(file_path)
    folder_name = folder_name or os.path.dirname(file_path)
    parent_name = os.path.basename(folder_name)
    grandparent_name = os.path.basename(os.path.dirname(folder_name)) if folder_name else ""

    # 1. 尝试从路径中提取 TMDb ID
    tmdb_id = _extract_tmdb_id(file_name)
    if not tmdb_id:
        tmdb_id = _extract_tmdb_id(parent_name)
    if not tmdb_id:
        tmdb_id = _extract_tmdb_id(grandparent_name)

    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY, '')

    # --- 情况 A: 提取到 TMDb ID，直接根据源类型获取标题 ---
    if tmdb_id:
        try:
            if not api_key:
                return tmdb_id, None, None

            # 根据源目录类型，选择对应的详情接口
            if source_type == 'movie':
                details = tmdb_handler.get_movie_details(int(tmdb_id), api_key)
                if details and details.get('title'):
                    return tmdb_id, 'movie', details['title']
            elif source_type == 'tv':
                details = tmdb_handler.get_tv_details(int(tmdb_id), api_key)
                if details and details.get('name'):
                    return tmdb_id, 'tv', details['name']
            else:  # mixed：先尝试电影，再尝试电视剧
                movie = tmdb_handler.get_movie_details(int(tmdb_id), api_key)
                if movie and movie.get('title'):
                    return tmdb_id, 'movie', movie['title']
                tv = tmdb_handler.get_tv_details(int(tmdb_id), api_key)
                if tv and tv.get('name'):
                    return tmdb_id, 'tv', tv['name']

            # 如果明确类型但查不到，返回空标题
            return tmdb_id, None, None
        except Exception as e:
            logger.warning(f"TMDb 查询 {tmdb_id} 失败: {e}")
            return tmdb_id, None, None

    # --- 情况 B: 没有 TMDb ID，通过文件名搜索 ---
    season_num, episode_num, media_type = _parse_video_filename(file_name)
    # 根据源目录类型修正媒体类型
    if source_type == 'movie':
        media_type = 'movie'
    elif source_type == 'tv':
        media_type = 'tv'
    # mixed 则保留解析结果，若未解析到季集信息则默认 movie

    # 构造搜索关键词（清理文件名）
    search_query = file_name
    for ext in VIDEO_EXTENSIONS:
        if search_query.endswith(f'.{ext}'):
            search_query = search_query[:-len(ext)-1]
            break

    search_query = re.sub(r'[sS]\d{1,4}[eE]\d{1,4}', '', search_query)
    search_query = re.sub(r'第\d+季', '', search_query)
    search_query = re.sub(r'第\d+集', '', search_query)
    search_query = re.sub(r'\[.*?\]', '', search_query)
    search_query = re.sub(r'\(.*?\)', '', search_query)
    search_query = re.sub(r'\d{4}', '', search_query)
    search_query = re.sub(r'\s+', ' ', search_query).strip()

    if not search_query or not api_key:
        return None, None, None

    # 按源类型限制搜索类别
    if source_type == 'movie':
        results = tmdb_handler.search_media(search_query, api_key, item_type='movie')
    elif source_type == 'tv':
        results = tmdb_handler.search_media(search_query, api_key, item_type='tv')
    else:  # mixed
        results = tmdb_handler.search_media(search_query, api_key, item_type=media_type)

    if results:
        return str(results[0].get('id')), media_type, results[0].get('title') or results[0].get('name')

    return None, None, None

def _match_rule(tmdb_id: str, media_type: str) -> Tuple[Optional[str], Optional[str]]:
    """匹配分类规则，返回目标分类 CID 和名称 (本地整理可不用网盘CID，但保留兼容)"""
    raw_rules = settings_db.get_setting('p115_sorting_rules')
    if not raw_rules:
        return None, None

    rules = raw_rules if isinstance(raw_rules, list) else []
    if isinstance(raw_rules, str):
        try:
            rules = json.loads(raw_rules)
        except:
            rules = []

    if not rules:
        return None, None

    rules = [r for r in rules if r.get('enabled', True) and r.get('cid')]

    if len(rules) == 1:
        return str(rules[0]['cid']), rules[0].get('category_path') or rules[0].get('dir_name')

    # 多规则时尝试匹配（仅当 SmartOrganizer 可用时）
    try:
        from handler.p115_service import SmartOrganizer
        organizer = SmartOrganizer(None, tmdb_id, media_type, "")
        organizer.raw_metadata = organizer._fetch_raw_metadata()
        for rule in rules:
            if organizer._match_rule(rule):
                return str(rule['cid']), rule.get('category_path') or rule.get('dir_name')
    except Exception as e:
        logger.warning(f"规则匹配异常，回退到第一条规则: {e}")
        return str(rules[0]['cid']), rules[0].get('category_path') or rules[0].get('dir_name')

    return None, None

def _get_rename_config() -> dict:
    return settings_db.get_setting('p115_rename_config') or {
        "main_title_lang": "zh",
        "main_year_en": True,
        "main_tmdb_fmt": "{tmdb=ID}",
        "season_fmt": "Season {02}",
        "file_format": ['title_zh', 'sep_dash_space', 'year'],
    }

def _build_target_path(target_base: str, category_path: str, tmdb_id: str, media_type: str,
                   title: str, season: int = None, episode: int = None, original_name: str = None) -> str:
    if not title:
        title = original_name or "Unknown"

    rename_config = _get_rename_config()

    if media_type == "tv":
        season_str = f"Season {season:02d}" if season else "Season 01"
        if episode:
            base_name = f"{title}.S{season:02d}E{episode:02d}" if season else f"{title}.E{episode:02d}"
        else:
            base_name = title
        target_dir = os.path.join(target_base, category_path or "未分类", title, season_str)
    else:
        year_match = re.search(r'(19|20)\d{2}', original_name or "")
        year = year_match.group(0) if year_match else ""
        file_format = rename_config.get('file_format', ['title_zh', 'sep_dash_space', 'year'])
        parts = []
        for part in file_format:
            if part == 'title_zh':
                parts.append(title)
            elif part == 'title_en':
                parts.append(title)
            elif part == 'year' and year:
                parts.append(year)
            elif part == 'sep_dash_space':
                parts.append(' - ')
            elif part == 'sep_middot_space':
                parts.append(' · ')
        base_name = ''.join(parts)
        target_dir = os.path.join(target_base, category_path or "电影", title)

    os.makedirs(target_dir, exist_ok=True)
    ext = os.path.splitext(original_name)[1] if original_name else '.mp4'
    target_path = os.path.join(target_dir, f"{base_name}{ext}")

    counter = 1
    while os.path.exists(target_path):
        base_name_ext = f"{base_name}_v{counter}{ext}"
        target_path = os.path.join(target_dir, base_name_ext)
        counter += 1

    return target_path

def _organize_file(src_path: str, target_path: str, mode: str) -> bool:
    target_dir = os.path.dirname(target_path)
    os.makedirs(target_dir, exist_ok=True)

    if os.path.exists(target_path):
        if mode == 'hardlink':
            os.remove(target_path)
        elif mode == 'copy':
            os.remove(target_path)
        # move 模式下目标已存在会报错，此处不删除（由 shutil.move 决定）

    try:
        if mode == 'hardlink':
            os.link(src_path, target_path)
        elif mode == 'copy':
            shutil.copy2(src_path, target_path)
        elif mode == 'move':
            shutil.move(src_path, target_path)
        return True
    except Exception as e:
        logger.error(f"整理文件失败: {e}")
        return False

def _add_record(file_id: str, original_name: str, renamed_name: str, status: str,
               tmdb_id: str, media_type: str, target_cid: str, category_name: str,
               source: str = 'local'):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO p115_organize_records
                    (file_id, original_name, renamed_name, status, tmdb_id, media_type, target_cid, category_name, fail_reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (file_id) DO UPDATE SET
                        original_name = EXCLUDED.original_name,
                        renamed_name = EXCLUDED.renamed_name,
                        status = EXCLUDED.status,
                        tmdb_id = EXCLUDED.tmdb_id,
                        media_type = EXCLUDED.media_type,
                        target_cid = EXCLUDED.target_cid,
                        category_name = EXCLUDED.category_name,
                        processed_at = NOW()
                """, (file_id, original_name, renamed_name, status, tmdb_id, media_type, target_cid, category_name, source))
                conn.commit()
    except Exception as e:
        logger.warning(f"添加记录失败: {e}")

def _scrape_file(file_path: str):
    try:
        from core_processor import MediaProcessor
        processor = MediaProcessor()
        processor.process_file_actively(file_path)
    except Exception as e:
        logger.warning(f"刮削失败: {e}")

def _scan_directory(directory: str, extensions: set = None) -> List[str]:
    if extensions is None:
        extensions = VIDEO_EXTENSIONS
    files = []
    if not os.path.exists(directory):
        return files
    for root, dirs, filenames in os.walk(directory):
        for filename in filenames:
            ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
            if ext in extensions:
                files.append(os.path.join(root, filename))
    return files

def _process_single_file(file_path: str, source_type: str, config: dict, target_base: str, mode: str,
                         auto_scrape: bool, rename_config: dict) -> bool:
    """处理单个文件
    Args:
        file_path: 源文件完整路径
        source_type: 来源类型 ('movie', 'tv', 'mixed')
        config: 全局配置字典
        target_base: 目标根目录
        mode: 整理模式 (hardlink/copy/move)
        auto_scrape: 是否自动刮削
        rename_config: 重命名规则配置
    Returns:
        是否成功整理
    """
    try:
        original_name = os.path.basename(file_path)
        folder_name = os.path.dirname(file_path)

        # ★ 关键调用：传递 source_type 以定向识别
        tmdb_id, media_type, title = _identify_media(file_path, source_type, folder_name)

        if not tmdb_id:
            _add_record(file_path, original_name, '', 'unrecognized', None, media_type,
                        '', '', 'local')
            return False

        # 匹配分类规则（可能使用115规则，若无则返回空）
        target_cid, category_path = _match_rule(tmdb_id, media_type)

        # 解析季集号
        season_num, episode_num, _ = _parse_video_filename(original_name)

        # 构建目标路径
        target_path = _build_target_path(
            target_base, category_path, tmdb_id, media_type, title,
            season_num, episode_num, original_name
        )

        # 执行文件操作
        if _organize_file(file_path, target_path, mode):
            renamed_name = os.path.basename(target_path)
            _add_record(file_path, original_name, renamed_name, 'success', tmdb_id, media_type,
                        target_cid or '', category_path or '', 'local')

            if auto_scrape:
                _scrape_file(target_path)

            return True

        return False
    except Exception as e:
        logger.error(f"  ➜ 处理文件异常 {file_path}: {e}")
        return False

def task_local_organize(processor=None):
    """本地文件整理主任务"""
    logger.info("=== 开始本地文件整理 ===")

    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager:
            task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

    config = get_config()
    enabled = config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_ENABLED, False)
    if not enabled:
        update_progress(100, "本地整理未启用")
        return

    source_movie = config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MOVIE, '')
    source_tv = config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_TV, '')
    source_mixed = config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MIXED, '')
    target_base = config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_TARGET_BASE, '')
    mode = config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_MODE, 'hardlink')
    auto_scrape = config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_AUTO_SCRAPE, True)
    max_workers = config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_MAX_WORKERS, 5)

    if not target_base:
        update_progress(100, "未配置目标目录")
        return

    # 构建源列表：每个元素是 (source_type, directory)
    sources = []
    if source_movie and os.path.exists(source_movie):
        sources.append(('movie', source_movie))
    if source_tv and os.path.exists(source_tv):
        sources.append(('tv', source_tv))
    if source_mixed and os.path.exists(source_mixed):
        sources.append(('mixed', source_mixed))

    if not sources:
        update_progress(100, "未配置源目录")
        return

    update_progress(5, "正在扫描源目录...")

    # 扫描所有文件，并记录它们的来源类型
    all_files = []  # 元素：(文件路径, source_type)
    for source_type, source_dir in sources:
        files = _scan_directory(source_dir)
        for f in files:
            all_files.append((f, source_type))
        logger.info(f"  ➜ [{source_type}] 扫描到 {len(files)} 个视频文件")

    if not all_files:
        update_progress(100, "未找到视频文件")
        return

    total = len(all_files)
    update_progress(10, f"共发现 {total} 个文件，开始整理...")

    processed = 0
    success = 0
    failed = 0
    rename_config = _get_rename_config()

    # 多线程处理
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for file_path, source_type in all_files:
            future = executor.submit(
                _process_single_file,   # 处理函数
                file_path,              # 参数1
                source_type,            # 参数2
                config,                 # 参数3
                target_base,            # 参数4
                mode,                   # 参数5
                auto_scrape,            # 参数6
                rename_config           # 参数7
            )
            futures[future] = file_path  # 仅用于日志关联

        # 收集结果
        for future in futures:
            try:
                result = future.result()
                processed += 1
                if result:
                    success += 1
                else:
                    failed += 1
                prog = 10 + int((processed / total) * 90)
                update_progress(prog, f"正在整理... ({processed}/{total})")
            except Exception as e:
                logger.error(f"  ➜ 处理异常: {e}")
                failed += 1

    final_msg = f"整理完成！成功 {success} 个，失败 {failed} 个"
    logger.info(f"=== {final_msg} ===")
    update_progress(100, final_msg)

# ============= 监控部分 =============
from watchdog.events import FileSystemEventHandler

class LocalOrganizeHandler(FileSystemEventHandler):
    def __init__(self, config: dict):
        super().__init__()
        self.config = config
        self.extensions = VIDEO_EXTENSIONS

    def _is_video(self, path: str) -> bool:
        ext = path.rsplit('.', 1)[-1].lower() if '.' in path else ''
        return ext in self.extensions

    def on_created(self, event):
        if not event.is_directory and self._is_video(event.src_path):
            logger.info(f"  ➜ [监控] 新文件: {event.src_path}")
            self._process(event.src_path)

    def on_modified(self, event):
        if not event.is_directory and self._is_video(event.src_path):
            logger.info(f"  ➜ [监控] 修改文件: {event.src_path}")
            self._process(event.src_path)

    def _process(self, file_path: str):
        # 监控模式下无法区分来源，默认作为 mixed 处理
        target_base = self.config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_TARGET_BASE, '')
        mode = self.config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_MODE, 'hardlink')
        auto_scrape = self.config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_AUTO_SCRAPE, True)
        rename_config = _get_rename_config()
        _process_single_file(file_path, 'mixed', self.config, target_base, mode, auto_scrape, rename_config)

def start_monitor():
    global _MONITOR_OBSERVER, _MONITOR_HANDLER
    if _MONITOR_OBSERVER:
        return {"success": True, "message": "监控已在运行"}

    config = get_config()
    if not config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_ENABLED, False):
        return {"success": False, "message": "本地整理未启用"}

    sources = []
    for key in [constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MOVIE,
                constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_TV,
                constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MIXED]:
        path = config.get(key, '')
        if path and os.path.exists(path):
            sources.append(path)

    if not sources:
        return {"success": False, "message": "未配置源目录"}

    from watchdog.observers import Observer
    _MONITOR_HANDLER = LocalOrganizeHandler(config)
    _MONITOR_OBSERVER = Observer()
    for path in sources:
        _MONITOR_OBSERVER.schedule(_MONITOR_HANDLER, path, recursive=True)
        logger.info(f"  ➜ [监控] 已监听: {path}")
    _MONITOR_OBSERVER.start()
    return {"success": True, "message": f"监控已启动，共 {len(sources)} 个目录"}

def stop_monitor():
    global _MONITOR_OBSERVER, _MONITOR_HANDLER
    if _MONITOR_OBSERVER:
        _MONITOR_OBSERVER.stop()
        _MONITOR_OBSERVER.join()
        _MONITOR_OBSERVER = None
        _MONITOR_HANDLER = None
        return {"success": True, "message": "监控已停止"}
    return {"success": True, "message": "监控未运行"}

def get_monitor_status() -> dict:
    return {"running": _MONITOR_OBSERVER is not None}
