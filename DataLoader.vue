<template>
  <div class="data-loader">
    <button @click="loadData" :disabled="isLoading">Load Data</button>
    <p v-if="isLoading">Loading...</p>
    <p v-if="errorMessage" class="error">{{ errorMessage }}</p>
  </div>
</template>

<script>
export default {
  data() {
    return {
      isLoading: false,
      errorMessage: '',
    };
  },
  methods: {
    async loadData() {
      this.isLoading = true;
      this.errorMessage = '';
      try {
        const response = await fetch('api/data-endpoint'); // 示例API调用
        const data = await response.json();
        this.$emit('data-loaded', data);
      } catch (error) {
        this.errorMessage = 'Error loading data.';
      } finally {
        this.isLoading = false;
      }
    },
  },
};
</script>

<style scoped>
.error {
  color: red;
}
</style>
