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

<template>
  <div class="home">
    <h1>Data Analysis Dashboard</h1>
    <DataLoader @data-loaded="updateData" />
    <Filter @filter-applied="applyFilter" />
    <DataTable :data="filteredData" />
    <Chart :chartData="chartData" :chartOptions="chartOptions" />
  </div>
</template>

<script>
import DataLoader from '@/components/DataLoader.vue';
import DataTable from '@/components/DataTable.vue';
import Chart from '@/components/Chart.vue';
import Filter from '@/components/Filter.vue';

export default {
  components: {
    DataLoader,
    DataTable,
    Chart,
    Filter,
  },
  data() {
    return {
      rawData: [],
      filteredData: [],
      chartData: {},
      chartOptions: {},
    };
  },
  methods: {
    updateData(data) {
      this.rawData = data;
      this.filteredData = data;
      this.updateChart();
    },
    applyFilter(searchText) {
      if (!searchText) {
        this.filteredData = this.rawData;
      } else {
        this.filteredData = this.rawData.filter(item => {
          return Object.values(item).some(val => String(val).includes(searchText));
        });
      }
      this.updateChart();
    },
    updateChart() {
      // Update chart data and options
      this.chartData = {
        labels: this.filteredData.map(item => item.name),
        datasets: [
          {
            label: 'Data',
            data: this.filteredData.map(item => item.value),
            borderColor: '#42A5F5',
            fill: false,
          },
        ],
      };
      this.chartOptions = {
        responsive: true,
        scales: {
          y: { beginAtZero: true },
        },
      };
    },
  },
};
</script>

<style scoped>
.home {
  padding: 20px;
}
</style>

