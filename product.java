public class Product {
    private String name;
    private String description;
    private double price;
    private int stockQuantity;

    // Constructor
    public Product(String name, String description, double price, int stockQuantity) {
        this.name = name;
        this.description = description;
        this.price = price;
        this.stockQuantity = stockQuantity;
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getStockQuantity() {
        return stockQuantity;
    }

    public void setStockQuantity(int stockQuantity) {
        this.stockQuantity = stockQuantity;
    }

    @Override
    public String toString() {
        return "Product{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", price=" + price +
                ", stockQuantity=" + stockQuantity +
                '}';
    }
}
import java.util.ArrayList;
import java.util.List;

public class ProductManager {
    private List<Product> products;

    public ProductManager() {
        products = new ArrayList<>();
    }

    public void addProduct(Product product) {
        products.add(product);
        System.out.println("Product added: " + product);
    }

    public void removeProduct(Product product) {
        products.remove(product);
        System.out.println("Product removed: " + product);
    }

    public void updateProduct(Product product, String name, String description, double price, int stockQuantity) {
        product.setName(name);
        product.setDescription(description);
        product.setPrice(price);
        product.setStockQuantity(stockQuantity);
        System.out.println("Product updated: " + product);
    }

    public List<Product> listProducts() {
        return products;
    }
}
public enum PriceRange {
    LOW,
    MEDIUM,
    HIGH
}
import java.util.ArrayList;
import java.util.List;

public class ProductManager {
    private List<Product> products;

    public ProductManager() {
        products = new ArrayList<>();
    }

    public void addProduct(Product product) {
        products.add(product);
        System.out.println("Product added: " + product);
    }

    public void removeProduct(Product product) {
        products.remove(product);
        System.out.println("Product removed: " + product);
    }

    public void updateProduct(Product product, String name, String description, double price, int stockQuantity) {
        product.setName(name);
        product.setDescription(description);
        product.setPrice(price);
        product.setStockQuantity(stockQuantity);
        System.out.println("Product updated: " + product);
    }

    public List<Product> listProducts() {
        return products;
    }

    public List<Product> searchByPriceRange(PriceRange priceRange) {
        List<Product> result = new ArrayList<>();
        for (Product product : products) {
            if (priceRange == PriceRange.LOW && product.getPrice() < 20) {
                result.add(product);
            } else if (priceRange == PriceRange.MEDIUM && product.getPrice() >= 20 && product.getPrice() <= 100) {
                result.add(product);
            } else if (priceRange == PriceRange.HIGH && product.getPrice() > 100) {
                result.add(product);
            }
        }
        return result;
    }
}

public class MainApp {
    public static void main(String[] args) {
        ProductManager productManager = new ProductManager();

        // Creating products
        Product product1 = new Product("Laptop", "High-performance laptop", 1200.0, 10);
        Product product2 = new Product("Smartphone", "Latest smartphone with great camera", 800.0, 50);
        Product product3 = new Product("Headphones", "Noise-cancelling headphones", 150.0, 100);

        // Adding products
        productManager.addProduct(product1);
        productManager.addProduct(product2);
        productManager.addProduct(product3);

        // Listing all products
        System.out.println("All Products:");
        for (Product product : productManager.listProducts()) {
            System.out.println(product);
        }

        // Searching products by price range
        System.out.println("\nLow Price Products:");
        for (Product product : productManager.searchByPriceRange(PriceRange.LOW)) {
            System.out.println(product);
        }

        // Updating product
        productManager.updateProduct(product1, "Laptop Pro", "Updated high-performance laptop", 1400.0, 5);

        // Removing product
        productManager.removeProduct(product3);

        // Listing products after removal and update
        System.out.println("\nProducts after removal and update:");
        for (Product product : productManager.listProducts()) {
            System.out.println(product);
        }
    }
}
import java.util.Scanner;

public class ProductManagerCLI {
    public static void main(String[] args) {
        ProductManager productManager = new ProductManager();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("Product Manager CLI");
            System.out.println("1. Add Product");
            System.out.println("2. Remove Product");
            System.out.println("3. List Products");
            System.out.println("4. Search by Price Range");
            System.out.println("5. Exit");
            System.out.print("Choose an option: ");

            int option = scanner.nextInt();
            scanner.nextLine(); // Consume newline

            if (option == 1) {
                System.out.print("Enter product name: ");
                String name = scanner.nextLine();
                System.out.print("Enter product description: ");
                String description = scanner.nextLine();
                System.out.print("Enter product price: ");
                double price = scanner.nextDouble();
                System.out.print("Enter stock quantity: ");
                int stockQuantity = scanner.nextInt();
                scanner.nextLine(); // Consume newline

                Product product = new Product(name, description, price, stockQuantity);
                productManager.addProduct(product);
            } else if (option == 2) {
                System.out.print("Enter name of product to remove: ");
                String name = scanner.nextLine();
                Product product = productManager.listProducts().stream()
                        .filter(p -> p.getName().equals(name))
                        .findFirst()
                        .orElse(null);
                if (product != null) {
                    productManager.removeProduct(product);
                } else {
                    System.out.println("Product not found.");
                }
            } else if (option == 3) {
                System.out.println("Listing all products:");
                for (Product product : productManager.listProducts()) {
                    System.out.println(product);
                }
            } else if (option == 4) {
                System.out.println("Search by Price Range");
                System.out.println("1. Low");
                System.out.println("2. Medium");
                System.out.println("3. High");
                System.out.print("Choose a range: ");
                int range = scanner.nextInt();
                PriceRange priceRange = range == 1 ? PriceRange.LOW : range == 2 ? PriceRange.MEDIUM : PriceRange.HIGH;

                System.out.println("Products in " + priceRange + " range:");
                for (Product product : productManager.searchByPriceRange(priceRange)) {
                    System.out.println(product);
                }
            } else if (option == 5) {
                break;
            } else {
                System.out.println("Invalid option. Try again.");
            }
        }

        scanner.close();
    }
}

public class Category {
    private String name;
    private String description;

    // Constructor
    public Category(String name, String description) {
        this.name = name;
        this.description = description;
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "Category{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}





