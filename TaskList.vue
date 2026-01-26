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

<template>
  <div class="task-manager">
    <h1>Task Manager</h1>
    <TaskSearch @search-tasks="searchTasks" />
    <TaskForm
      v-if="isEditing"
      :task="currentTask"
      formTitle="Edit Task"
      formButtonText="Save Changes"
      @submit-form="submitForm"
    />
    <TaskForm
      v-else
      :task="newTask"
      formTitle="Add New Task"
      formButtonText="Add Task"
      @submit-form="submitForm"
    />
    <TaskList :tasks="filteredTasks" @edit-task="startEditing" @delete-task="deleteTask" />
  </div>
</template>

<script>
import TaskSearch from '@/components/TaskSearch.vue';
import TaskForm from '@/components/TaskForm.vue';
import TaskList from '@/components/TaskList.vue';

export default {
  components: {
    TaskSearch,
    TaskForm,
    TaskList,
  },
  data() {
    return {
      tasks: [
        { id: 1, name: 'Task 1', description: 'This is the first task' },
        { id: 2, name: 'Task 2', description: 'This is the second task' },
      ],
      filteredTasks: [],
      isEditing: false,
      currentTask: { id: null, name: '', description: '' },
      newTask: { id: null, name: '', description: '' },
    };
  },
  methods: {
    searchTasks(searchText) {
      if (searchText) {
        this.filteredTasks = this.tasks.filter(task => {
          return task.name.toLowerCase().includes(searchText.toLowerCase());
        });
      } else {
        this.filteredTasks = this.tasks;
      }
    },
    startEditing(taskId) {
      this.isEditing = true;
      this.currentTask = this.tasks.find(task => task.id === taskId);
    },
    submitForm(taskData) {
      if (this.isEditing) {
        const index = this.tasks.findIndex(task => task.id === this.currentTask.id);
        this.tasks[index] = { ...taskData, id: this.currentTask.id };
      } else {
        taskData.id = this.tasks.length + 1;
        this.tasks.push(taskData);
      }
      this.resetForm();
    },
    deleteTask(taskId) {
      this.tasks = this.tasks.filter(task => task.id !== taskId);
    },
    resetForm() {
      this.isEditing = false;
      this.currentTask = { id: null, name: '', description: '' };
      this.newTask = { id: null, name: '', description: '' };
      this.filteredTasks = this.tasks;
    },
  },
  created() {
    this.filteredTasks = this.tasks;
  },
};
</script>

<style scoped>
.task-manager {
  padding: 20px;
}

h1 {
  margin-bottom: 20px;
}
</style>
<template>
  <div class="task-page">
    <TaskManager />
  </div>
</template>

<script>
import TaskManager from '@/components/TaskManager.vue';

export default {
  components: {
    TaskManager,
  },
};
</script>

<style scoped>
.task-page {
  padding: 20px;
}
</style>
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
    <p v-else>No tasks available.</p>
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
<template>
  <div class="task-manager">
    <h1>Task Manager</h1>
    <TaskSearch @search-tasks="searchTasks" />
    <TaskForm
      v-if="isEditing"
      :task="currentTask"
      formTitle="Edit Task"
      formButtonText="Save Changes"
      @submit-form="submitForm"
    />
    <TaskForm
      v-else
      :task="newTask"
      formTitle="Add New Task"
      formButtonText="Add Task"
      @submit-form="submitForm"
    />
    <TaskList :tasks="filteredTasks" @edit-task="startEditing" @delete-task="deleteTask" />
  </div>
</template>

<script>
import TaskSearch from '@/components/TaskSearch.vue';
import TaskForm from '@/components/TaskForm.vue';
import TaskList from '@/components/TaskList.vue';

export default {
  components: {
    TaskSearch,
    TaskForm,
    TaskList,
  },
  data() {
    return {
      tasks: [
        { id: 1, name: 'Task 1', description: 'This is the first task' },
        { id: 2, name: 'Task 2', description: 'This is the second task' },
      ],
      filteredTasks: [],
      isEditing: false,
      currentTask: { id: null, name: '', description: '' },
      newTask: { id: null, name: '', description: '' },
    };
  },
  methods: {
    searchTasks(searchText) {
      if (searchText) {
        this.filteredTasks = this.tasks.filter(task => {
          return task.name.toLowerCase().includes(searchText.toLowerCase());
        });
      } else {
        this.filteredTasks = this.tasks;
      }
    },
    startEditing(taskId) {
      this.isEditing = true;
      this.currentTask = this.tasks.find(task => task.id === taskId);
    },
    submitForm(taskData) {
      if (this.isEditing) {
        const index = this.tasks.findIndex(task => task.id === this.currentTask.id);
        this.tasks[index] = { ...taskData, id: this.currentTask.id };
      } else {
        taskData.id = this.tasks.length + 1;
        this.tasks.push(taskData);
      }
      this.resetForm();
    },
    deleteTask(taskId) {
      this.tasks = this.tasks.filter(task => task.id !== taskId);
    },
    resetForm() {
      this.isEditing = false;
      this.currentTask = { id: null, name: '', description: '' };
      this.newTask = { id: null, name: '', description: '' };
      this.filteredTasks = this.tasks;
    },
  },
  created() {
    this.filteredTasks = this.tasks;
  },
};
</script>

<style scoped>
.task-manager {
  padding: 20px;
}

h1 {
  margin-bottom: 20px;
}
</style>
<template>
  <div class="task-page">
    <TaskManager />
  </div>
</template>

<script>
import TaskManager from '@/components/TaskManager.vue';

export default {
  components: {
    TaskManager,
  },
};
</script>

<style scoped>
.task-page {
  padding: 20px;
}
</style>




