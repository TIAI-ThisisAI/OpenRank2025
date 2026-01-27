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
