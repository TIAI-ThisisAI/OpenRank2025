<template>
  <div class="user-list">
    <h2>User List</h2>
    <table v-if="users.length">
      <thead>
        <tr>
          <th>Name</th>
          <th>Email</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="user in users" :key="user.id">
          <td>{{ user.name }}</td>
          <td>{{ user.email }}</td>
          <td>
            <button @click="editUser(user.id)">Edit</button>
            <button @click="deleteUser(user.id)">Delete</button>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-else>No users available.</p>
  </div>
</template>

<script>
export default {
  props: {
    users: Array,
  },
  methods: {
    editUser(userId) {
      this.$emit('edit-user', userId);
    },
    deleteUser(userId) {
      this.$emit('delete-user', userId);
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
