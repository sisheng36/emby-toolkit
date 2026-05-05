<!-- src/components/LocalOrganizeConfigModal.vue -->
<template>
  <n-modal v-model:show="visible" preset="card" style="width: 750px; max-height: 80vh;" title="本地文件整理配置" :bordered="false">
    <n-tabs type="line" size="small" :default-value="'basic'">
      <!-- 基本设置 -->
      <n-tab-pane name="basic" tab="基本设置">
        <n-form ref="formRef" :model="form" label-placement="left" label-width="140">
          <n-form-item label="启用功能">
            <n-switch v-model:value="form.enabled" />
          </n-form-item>
          <n-form-item label="电影源目录">
            <n-input v-model:value="form.source_movie" placeholder="/path/to/movies" />
          </n-form-item>
          <n-form-item label="电视剧源目录">
            <n-input v-model:value="form.source_tv" placeholder="/path/to/tvshows" />
          </n-form-item>
          <n-form-item label="混合源目录">
            <n-input v-model:value="form.source_mixed" placeholder="/path/to/mixed" />
          </n-form-item>
          <n-form-item label="目标根目录">
            <n-input v-model:value="form.target_base" placeholder="/mnt/media" />
          </n-form-item>
          <n-form-item label="整理模式">
            <n-radio-group v-model:value="form.mode">
              <n-radio-button value="hardlink">硬链接</n-radio-button>
              <n-radio-button value="copy">复制</n-radio-button>
              <n-radio-button value="move">移动</n-radio-button>
            </n-radio-group>
          </n-form-item>
          <n-form-item label="自动刮削">
            <n-switch v-model:value="form.auto_scrape" />
          </n-form-item>
          <n-form-item label="并发数">
            <n-input-number v-model:value="form.max_workers" :min="1" :max="10" />
          </n-form-item>
        </n-form>
      </n-tab-pane>

      <!-- 分类规则 -->
      <n-tab-pane name="sorting" tab="分类规则">
        <n-space vertical>
          <n-text depth="3" style="font-size: 13px;">文件整理后将按此规则分配到目标目录下的子文件夹（为空则使用默认）。</n-text>
          <n-form label-placement="left" label-width="140">
            <n-form-item label="电影默认分类路径">
              <n-input v-model:value="sortingRules.movie" placeholder="例如：电影/自动整理" />
            </n-form-item>
            <n-form-item label="电视剧默认分类路径">
              <n-input v-model:value="sortingRules.tv" placeholder="例如：电视剧/自动整理" />
            </n-form-item>
            <n-form-item label="高级（JSON规则数组）">
              <n-input
                v-model:value="sortingRules.advancedJson"
                type="textarea"
                placeholder='[{"enabled":true,"media_type":"movie","category_path":"电影/合集","tmdb_ids":["123"]}]'
                rows="5"
                style="font-family: monospace;"
              />
              <n-text depth="3" style="font-size: 12px; margin-top: 4px;">
                留空则使用上面的简单规则。高级规则优先级更高。格式错误会被忽略。
              </n-text>
            </n-form-item>
          </n-form>
        </n-space>
      </n-tab-pane>

      <!-- 重命名配置 -->
      <n-tab-pane name="rename" tab="重命名配置">
        <n-form label-placement="left" label-width="140">
          <n-form-item label="主标题语言">
            <n-select
              v-model:value="renameConfig.main_title_lang"
              :options="[
                { label: '中文优先', value: 'zh' },
                { label: '英文优先', value: 'en' }
              ]"
            />
          </n-form-item>
          <n-form-item label="季文件夹格式">
            <n-input v-model:value="renameConfig.season_fmt" placeholder="Season {02}" />
            <n-text depth="3" style="font-size: 12px; margin-left: 8px;">{02} 为两位数字季号</n-text>
          </n-form-item>
          <n-form-item label="电影文件名格式">
            <n-checkbox-group v-model:value="renameConfig.file_format">
              <n-space item-style="display: block;">
                <n-checkbox value="title_zh" label="标题" />
                <n-checkbox value="sep_dash_space" label=" - " />
                <n-checkbox value="sep_middot_space" label=" · " />
                <n-checkbox value="year" label="年份" />
                <n-checkbox value="resolution" label="分辨率" />
                <n-checkbox value="codec" label="编码" />
                <n-checkbox value="audio" label="音轨" />
                <n-checkbox value="group" label="小组" />
              </n-space>
            </n-checkbox-group>
            <n-text depth="3" style="font-size: 12px; margin-top: 4px;">
              按顺序拼接，示例：标题 +  -  + 年份 → "盗梦空间 - 2010.mp4"
            </n-text>
          </n-form-item>
        </n-form>
      </n-tab-pane>
    </n-tabs>

    <template #footer>
      <n-space justify="end">
        <n-button @click="closeModal">取消</n-button>
        <n-button type="primary" @click="saveAllConfig" :loading="saving">保存所有设置</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, reactive, watch, computed } from 'vue'
import {
  NModal, NTabs, NTabPane, NForm, NFormItem, NSwitch, NInput, NInputNumber,
  NRadioGroup, NRadioButton, NSpace, NButton, NText, NSelect, NCheckboxGroup, NCheckbox
} from 'naive-ui'
import axios from 'axios'

const props = defineProps({ show: Boolean })
const emit = defineEmits(['update:show', 'saved'])

const visible = computed({
  get: () => props.show,
  set: (val) => emit('update:show', val)
})

const formRef = ref(null)
const saving = ref(false)

// 基本配置
const form = reactive({
  enabled: false,
  source_movie: '',
  source_tv: '',
  source_mixed: '',
  target_base: '',
  mode: 'hardlink',
  auto_scrape: true,
  max_workers: 5
})

// 分类规则（简单模式 + 高级JSON）
const sortingRules = reactive({
  movie: '',
  tv: '',
  advancedJson: ''
})

// 重命名配置
const renameConfig = reactive({
  main_title_lang: 'zh',
  season_fmt: 'Season {02}',
  file_format: ['title_zh', 'sep_dash_space', 'year']
})

// 加载所有配置
watch(() => props.show, async (val) => {
  if (val) {
    await Promise.all([loadBasicConfig(), loadSortingRules(), loadRenameConfig()])
  }
})

async function loadBasicConfig() {
  try {
    const res = await axios.get('/api/p115/local_organize/config')
    if (res.data.success) Object.assign(form, res.data.data)
  } catch (e) {
    console.error('加载基本配置失败', e)
  }
}

async function loadSortingRules() {
  try {
    const res = await axios.get('/api/p115/local_organize/sorting_rules')
    if (res.data.success) {
      const rules = res.data.data || []
      // 尝试拆分为简单规则和高级规则
      let movieRule = rules.find(r => r.media_type === 'movie' && !r.tmdb_ids && r.enabled !== false)
      let tvRule = rules.find(r => r.media_type === 'tv' && !r.tmdb_ids && r.enabled !== false)
      sortingRules.movie = movieRule?.category_path || ''
      sortingRules.tv = tvRule?.category_path || ''
      // 其余规则作为高级JSON
      const advanced = rules.filter(r => !(r.media_type === 'movie' && !r.tmdb_ids) && !(r.media_type === 'tv' && !r.tmdb_ids))
      sortingRules.advancedJson = advanced.length ? JSON.stringify(advanced, null, 2) : ''
    }
  } catch (e) {
    console.error('加载分类规则失败', e)
  }
}

async function loadRenameConfig() {
  try {
    const res = await axios.get('/api/p115/local_organize/rename_config')
    if (res.data.success) Object.assign(renameConfig, res.data.data)
  } catch (e) {
    console.error('加载重命名配置失败', e)
  }
}

async function saveAllConfig() {
  saving.value = true
  try {
    // 保存基本配置
    await axios.post('/api/p115/local_organize/config', form)
    // 构建分类规则数组
    const rules = []
    if (sortingRules.movie) rules.push({ enabled: true, media_type: 'movie', category_path: sortingRules.movie })
    if (sortingRules.tv) rules.push({ enabled: true, media_type: 'tv', category_path: sortingRules.tv })
    if (sortingRules.advancedJson) {
      try {
        const advanced = JSON.parse(sortingRules.advancedJson)
        if (Array.isArray(advanced)) rules.push(...advanced)
      } catch (e) {
        window.$message.warning('高级分类规则 JSON 格式错误，已忽略')
      }
    }
    await axios.post('/api/p115/local_organize/sorting_rules', rules)
    // 保存重命名配置
    await axios.post('/api/p115/local_organize/rename_config', renameConfig)
    window.$message.success('所有配置已保存')
    emit('saved')
  } catch (e) {
    window.$message.error('保存失败')
  } finally {
    saving.value = false
  }
}

function closeModal() {
  emit('update:show', false)
}
</script>
