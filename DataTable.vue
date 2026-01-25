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

<template>
  <div class="chart-container">
    <canvas ref="chart"></canvas>
  </div>
</template>

<script>
import { Chart } from 'chart.js';

export default {
  props: {
    chartData: Object,
    chartOptions: Object,
  },
  mounted() {
    this.renderChart();
  },
  methods: {
    renderChart() {
      if (this.chartData && this.chartOptions) {
        new Chart(this.$refs.chart, {
          type: 'line',  // Chart type (line, bar, etc.)
          data: this.chartData,
          options: this.chartOptions,
        });
      }
    },
  },
};
</script>

<style scoped>
.chart-container {
  position: relative;
  height: 400px;
  width: 100%;
}
</style>
