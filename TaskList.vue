<template>
  <div class="task-list">
    <h2>Task List</h2>
    <table v-if="tasks.length">
      <thead>
        <tr>
          <th>Task Name</th>
          <th>Description</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="task in tasks" :key="task.id">
          <td>{{ task.name }}</td>
          <td>{{ task.description }}</td>
          <td>
            <button @click="editTask(task.id)">Edit</button>
            <button @click="deleteTask(task.id)">Delete</button>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-else>No tasks available</p>
  </div>
</template>

<script>
export default {
  props: {
    tasks: Array,
  },
  methods: {
    editTask(taskId) {
      this.$emit('edit-task', taskId);
    },
    deleteTask(taskId) {
      this.$emit('delete-task', taskId);
    },
  },
};
</script>

<style scoped>
table {
  width: 100%;
  border-collapse: collapse;
}

th, td {
  padding: 8px;
  text-align: left;
}

button {
  margin-right: 5px;
}
</style>

<template>
  <div class="task-search">
    <input v-model="searchText" placeholder="Search tasks..." @input="applySearch" />
  </div>
</template>

<script>
export default {
  data() {
    return {
      searchText: '',
    };
  },
  methods: {
    applySearch() {
      this.$emit('search-tasks', this.searchText);
    },
  },
};
</script>

<style scoped>
.task-search {
  margin-bottom: 10px;
}

input {
  padding: 8px;
  width: 200px;
}
</style>
<template>
  <div class="task-form">
    <h2>{{ formTitle }}</h2>
    <form @submit.prevent="handleSubmit">
      <label for="name">Task Name</label>
      <input v-model="task.name" id="name" type="text" required />

      <label for="description">Description</label>
      <input v-model="task.description" id="description" type="text" required />

      <button type="submit">{{ formButtonText }}</button>
    </form>
  </div>
</template>

<script>
export default {
  props: {
    task: Object,
    formTitle: String,
    formButtonText: String,
  },
  methods: {
    handleSubmit() {
      this.$emit('submit-form', this.task);
    },
  },
};
</script>

<style scoped>
form {
  display: flex;
  flex-direction: column;
}

label {
  margin: 5px 0;
}

input {
  padding: 8px;
  margin-bottom: 10px;
  width: 200px;
}
</style>

