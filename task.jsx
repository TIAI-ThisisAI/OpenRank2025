import React from 'react';

function TaskList({ tasks, onEdit, onDelete }) {
  return (
    <div className="task-list">
      <h2>Task List</h2>
      {tasks.length > 0 ? (
        <table>
          <thead>
            <tr>
              <th>Task Name</th>
              <th>Description</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {tasks.map(task => (
              <tr key={task.id}>
                <td>{task.name}</td>
                <td>{task.description}</td>
                <td>
                  <button onClick={() => onEdit(task.id)}>Edit</button>
                  <button onClick={() => onDelete(task.id)}>Delete</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p>No tasks available.</p>
      )}
    </div>
  );
}

export default TaskList;

function TaskSearch({ onSearch }) {
  const [searchText, setSearchText] = useState('');

  const handleSearch = (event) => {
    setSearchText(event.target.value);
    onSearch(event.target.value);
  };

  return (
    <div className="task-search">
      <input
        type="text"
        placeholder="Search tasks..."
        value={searchText}
        onChange={handleSearch}
      />
    </div>
  );
}

export default TaskSearch;
import React, { useState, useEffect } from 'react';

function TaskForm({ task, onSubmit, buttonText }) {
  const [formData, setFormData] = useState({
    name: task?.name || '',
    description: task?.description || '',
    priority: task?.priority || 'low',
  });

  useEffect(() => {
    if (task) {
      setFormData({
        name: task.name,
        description: task.description,
        priority: task.priority,
      });
    }
  }, [task]);

  const handleChange = (event) => {
    const { name, value } = event.target;
    setFormData((prevData) => ({
      ...prevData,
      [name]: value,
    }));
  };

  const handleSubmit = (event) => {
    event.preventDefault();
    onSubmit(formData);
  };

  return (
    <div className="task-form">
      <h2>{buttonText}</h2>
      <form onSubmit={handleSubmit}>
        <label>Task Name</label>
        <input
          type="text"
          name="name"
          value={formData.name}
          onChange={handleChange}
          required
        />
        <label>Description</label>
        <input
          type="text"
          name="description"
          value={formData.description}
          onChange={handleChange}
          required
        />
        <label>Priority</label>
        <select
          name="priority"
          value={formData.priority}
          onChange={handleChange}
        >
          <option value="low">Low</option>
          <option value="medium">Medium</option>
          <option value="high">High</option>
        </select>
        <button type="submit">{buttonText}</button>
      </form>
    </div>
  );
}

export default TaskForm;

