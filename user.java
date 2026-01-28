public class User {
    private String name;
    private String email;
    private String role;

    // Constructor
    public User(String name, String email, String role) {
        this.name = name;
        this.email = email;
        this.role = role;
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", email='" + email + '\'' +
                ", role='" + role + '\'' +
                '}';
    }
}
import java.util.ArrayList;
import java.util.List;

public class UserManager {
    private List<User> users;

    public UserManager() {
        users = new ArrayList<>();
    }

    public void addUser(User user) {
        users.add(user);
        System.out.println("User added: " + user);
    }

    public void removeUser(User user) {
        users.remove(user);
        System.out.println("User removed: " + user);
    }

    public List<User> listUsers() {
        return users;
    }
}
public enum UserRole {
    ADMIN,
    USER,
    GUEST
}
public class MainApp {
    public static void main(String[] args) {
        UserManager userManager = new UserManager();

        // Creating users
        User user1 = new User("Alice", "alice@example.com", UserRole.ADMIN.name());
        User user2 = new User("Bob", "bob@example.com", UserRole.USER.name());

        // Adding users
        userManager.addUser(user1);
        userManager.addUser(user2);

        // Listing users
        System.out.println("All users:");
        for (User user : userManager.listUsers()) {
            System.out.println(user);
        }

        // Removing a user
        userManager.removeUser(user1);

        // Listing users after removal
        System.out.println("Users after removal:");
        for (User user : userManager.listUsers()) {
            System.out.println(user);
        }
    }
}
public class UserManager {
    private List<User> users;

    public UserManager() {
        users = new ArrayList<>();
    }

    public void addUser(User user) {
        if (user == null || user.getName() == null || user.getEmail() == null || user.getRole() == null) {
            System.out.println("Invalid user. Cannot add.");
            return;
        }
        users.add(user);
        System.out.println("User added: " + user);
    }

    public void removeUser(User user) {
        users.remove(user);
        System.out.println("User removed: " + user);
    }

    public List<User> listUsers() {
        return users;
    }
}
public class UserManager {
    private List<User> users;

    public UserManager() {
        users = new ArrayList<>();
    }

    public void addUser(User user) {
        if (user == null || user.getName() == null || user.getEmail() == null || user.getRole() == null) {
            System.out.println("Invalid user. Cannot add.");
            return;
        }
        users.add(user);
        System.out.println("User added: " + user);
    }

    public void removeUser(User user) {
        users.remove(user);
        System.out.println("User removed: " + user);
    }

    public List<User> listUsers() {
        return users;
    }

    public List<User> searchByRole(UserRole role) {
        List<User> result = new ArrayList<>();
        for (User user : users) {
            if (user.getRole().equals(role.name())) {
                result.add(user);
            }
        }
        return result;
    }
}
import java.util.Scanner;

public class UserManagerCLI {
    public static void main(String[] args) {
        UserManager userManager = new UserManager();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("User Manager CLI");
            System.out.println("1. Add User");
            System.out.println("2. Remove User");
            System.out.println("3. List Users");
            System.out.println("4. Exit");
            System.out.print("Choose an option: ");

            int option = scanner.nextInt();
            scanner.nextLine(); // Consume newline

            if (option == 1) {
                System.out.print("Enter name: ");
                String name = scanner.nextLine();
                System.out.print("Enter email: ");
                String email = scanner.nextLine();
                System.out.print("Enter role (ADMIN, USER, GUEST): ");
                String role = scanner.nextLine();

                User user = new User(name, email, role);
                userManager.addUser(user);
            } else if (option == 2) {
                System.out.print("Enter name of user to remove: ");
                String name = scanner.nextLine();
                User user = userManager.listUsers().stream()
                        .filter(u -> u.getName().equals(name))
                        .findFirst()
                        .orElse(null);
                if (user != null) {
                    userManager.removeUser(user);
                } else {
                    System.out.println("User not found.");
                }
            } else if (option == 3) {
                System.out.println("Listing all users:");
                for (User user : userManager.listUsers()) {
                    System.out.println(user);
                }
            } else if (option == 4) {
                break;
            } else {
                System.out.println("Invalid option. Try again.");
            }
        }

        scanner.close();
    }
}






