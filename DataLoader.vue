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

<template>
  <div class="data-table">
    <table v-if="data && data.length">
      <thead>
        <tr>
          <th v-for="(value, key) in data[0]" :key="key">{{ key }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(row, index) in data" :key="index">
          <td v-for="(value, key) in row" :key="key">{{ value }}</td>
        </tr>
      </tbody>
    </table>
    <p v-else>No data available</p>
  </div>
</template>

<script>
export default {
  props: {
    data: Array,
  },
};
</script>

<style scoped>
table {
  width: 100%;
  border-collapse: collapse;
}

table, th, td {
  border: 1px solid black;
}

th, td {
  padding: 8px;
  text-align: left;
}
</style>
