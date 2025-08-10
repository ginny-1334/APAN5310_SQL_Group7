
-- ============================
-- ABC Foodmart Database Schema
-- ============================

-- Store Table
-- Stores details such as location, operating hours, and assigned manager.
CREATE TABLE Store (
    store_id SERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    zipcode VARCHAR(10) NOT NULL,
    operating_hours VARCHAR(100) NOT NULL,
    manager_id INTEGER UNIQUE 
);

-- Department Table
-- Represents departments within each store (ex. Bakery, Dairy)
CREATE TABLE Department (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL
);

-- Employee Table
-- Tracks employee records including contact, role, and department assignment.
-- Each employee is linked to a store and department.
CREATE TABLE Employee (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    phone VARCHAR(20) NOT NULL,
    role VARCHAR(50) NOT NULL,
    store_id INTEGER REFERENCES Store(store_id) ON DELETE CASCADE,
    department_id INTEGER REFERENCES Department(department_id)
);

-- Add foreign key for store manager after Employee table exists
-- Easy to find who is responsible for managing the store
ALTER TABLE Store
ADD CONSTRAINT fk_store_manager FOREIGN KEY (manager_id) REFERENCES Employee(employee_id);

-- ShiftSchedule Table
-- Defines work schedules 
CREATE TABLE ShiftSchedule (
    schedule_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES Employee(employee_id) ON DELETE CASCADE,
    shift_date DATE NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL
);


-- Category Table
-- Categorizes products by type (ex. frozen, fresh, beverage)
CREATE TABLE Category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);

-- Product Table
-- Stores product details including SKU, name, brand, and shelf location.
CREATE TABLE Product (
    sku varchar(20) PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    brand VARCHAR(100) NOT NULL,
    shelf_location VARCHAR(50) NOT NULL,
    category_id INTEGER REFERENCES Category(category_id)
);

-- ProductPricing Table
-- Maintains a history of regular and promotional pricing by date.
CREATE TABLE ProductPricing (
    sku varchar(20) REFERENCES Product(sku),
    price_date DATE NOT NULL,
    regular_price NUMERIC(10,2) NOT NULL,
    promo_price NUMERIC(10,2),
    PRIMARY KEY (sku, price_date)
);

-- Inventory Table
-- Tracks stock per store
CREATE TABLE Inventory (
    inventory_id SERIAL PRIMARY KEY,
    store_id INTEGER REFERENCES Store(store_id),
    sku varchar(20) REFERENCES Product(sku),
    quantity_on_hand INTEGER NOT NULL,
    reorder_threshold INTEGER NOT NULL,
    restock_status VARCHAR(20) CHECK (restock_status IN ('Restock Needed', 'In Stock')) NOT NULL,
    UNIQUE (store_id, sku)
);

-- Vendor Table
-- Holds vendor name and tier information
CREATE TABLE Vendor (
    vendor_id SERIAL PRIMARY KEY,
    vendor_name VARCHAR(100) NOT NULL,
    vendor_tier VARCHAR(10) NOT NULL
);

-- VendorProduct Table 
-- Associates vendors with the products they supply.
CREATE TABLE VendorProduct (
    vendor_id INTEGER REFERENCES Vendor(vendor_id) ON DELETE CASCADE,
    sku varchar(20) REFERENCES Product(sku),
    PRIMARY KEY (vendor_id, sku)
);

-- Delivery Table
-- Records vendor deliveries by date and receiving store
CREATE TABLE Delivery (
    delivery_id SERIAL PRIMARY KEY,
    vendor_id INTEGER REFERENCES Vendor(vendor_id),
    store_id INTEGER REFERENCES Store(store_id),
    delivery_date DATE NOT NULL,
    status VARCHAR(20) CHECK (status IN ('Completed', 'Delayed')) NOT NULL
);

-- DeliveryItem Table 
-- Record of products delivered per delivery event
CREATE TABLE DeliveryItem (
    delivery_id INTEGER REFERENCES Delivery(delivery_id) ON DELETE CASCADE,
    sku varchar(20) REFERENCES Product(sku),
    quantity INTEGER NOT NULL,
    PRIMARY KEY (delivery_id, sku)
);


-- Promotion Table
-- Defines promotional campaigns with start and end dates
CREATE TABLE Promotion (
    promo_id SERIAL PRIMARY KEY,
    sku varchar(20) REFERENCES Product(sku),
    start_date DATE,
    end_date DATE,
    discount_amount NUMERIC
);

-- Sales Table
-- Captures each sales transaction with payment method and promotion details
CREATE TABLE Sale (
    sale_id SERIAL PRIMARY KEY,
    store_id INTEGER REFERENCES Store(store_id),
    sale_datetime TIMESTAMP,
    payment_type VARCHAR(20) CHECK (payment_type IN ('Cash', 'Credit Card', 'Mobile'))
);

-- SaleItem Table
-- Details of each item sold in a transaction
CREATE TABLE SaleItem (
    sale_id INTEGER REFERENCES Sale(sale_id) ON DELETE CASCADE,
    sku varchar(20) REFERENCES Product(sku),
    quantity_sold INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    promo_applied BOOLEAN DEFAULT FALSE,
    promo_discount NUMERIC(10,2),
    promo_id INTEGER REFERENCES Promotion(promo_id),
    PRIMARY KEY (sale_id, sku)
);


-- Expense Table
-- Tracks store-level operating expenses by category.
CREATE TABLE Expense (
    expense_id SERIAL PRIMARY KEY,
    store_id INTEGER REFERENCES Store(store_id),
    expense_date DATE NOT NULL,
    expense_category VARCHAR(50) CHECK (expense_category IN ('Wages', 'Utilities', 'Spoilage', 'Other')) NOT NULL,
    amount NUMERIC(12,2) NOT NULL
);


-- ReturnReason Table
-- Table for categorizing return types.
CREATE TABLE ReturnReason (
    reason_code VARCHAR(10) PRIMARY KEY,
    description TEXT
);

-- ProductReturn Table
-- Logs product returns and links to defined return reasons
CREATE TABLE ProductReturn (
    return_id SERIAL PRIMARY KEY,
    sale_id INTEGER REFERENCES Sale(sale_id),
    sku varchar(20) REFERENCES Product(sku),
    return_date DATE NOT NULL,
    quantity_returned INTEGER NOT NULL,
    reason_code VARCHAR(10) REFERENCES ReturnReason(reason_code)
);

-- =========================================

-- Trigger Function: Auto-update restock_status
CREATE OR REPLACE FUNCTION update_restock_status()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.quantity_on_hand < NEW.reorder_threshold THEN
        NEW.restock_status := 'Restock Needed';
    ELSE
        NEW.restock_status := 'In Stock';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER trg_update_restock_status
BEFORE INSERT OR UPDATE ON Inventory
FOR EACH ROW
EXECUTE FUNCTION update_restock_status();



-- Trigger function : SaleItem inventory deduction
-- Deducts inventory after a sale is recorded
CREATE OR REPLACE FUNCTION deduct_inventory_after_sale()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE Inventory
  SET quantity_on_hand = quantity_on_hand - NEW.quantity_sold
  WHERE store_id = (SELECT store_id FROM Sale WHERE sale_id = NEW.sale_id)
    AND sku = NEW.sku;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER trg_deduct_inventory
AFTER INSERT ON SaleItem
FOR EACH ROW
EXECUTE FUNCTION deduct_inventory_after_sale();


-- Trigger function
-- Adds inventory back when a product is returned
-- This function updates the inventory when a product is returned.
CREATE OR REPLACE FUNCTION add_inventory_on_return()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE Inventory
  SET quantity_on_hand = quantity_on_hand + NEW.quantity_returned
  WHERE store_id = (SELECT store_id FROM Sale WHERE sale_id = NEW.sale_id)
    AND sku = NEW.sku;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER trg_add_inventory_on_return
AFTER INSERT ON ProductReturn
FOR EACH ROW
EXECUTE FUNCTION add_inventory_on_return();

-- Trigger Function: Add inventory when delivery is received
CREATE OR REPLACE FUNCTION add_inventory_on_delivery()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the Inventory record already exists
    IF EXISTS (
        SELECT 1 FROM Inventory 
        WHERE store_id = (SELECT store_id FROM Delivery WHERE delivery_id = NEW.delivery_id)
          AND sku = NEW.sku
    ) THEN
        -- If exists, increase quantity_on_hand
        UPDATE Inventory
        SET quantity_on_hand = quantity_on_hand + NEW.quantity
        WHERE store_id = (SELECT store_id FROM Delivery WHERE delivery_id = NEW.delivery_id)
          AND sku = NEW.sku;
    ELSE
        -- If not exists, insert a new record (default reorder_threshold = 10)
        INSERT INTO Inventory (store_id, sku, quantity_on_hand, reorder_threshold, restock_status)
        VALUES (
            (SELECT store_id FROM Delivery WHERE delivery_id = NEW.delivery_id),
            NEW.sku,
            NEW.quantity,
            10,  -- Default threshold, adjust as needed
            'In Stock'
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: run after a DeliveryItem is inserted
CREATE TRIGGER trg_add_inventory_on_delivery
AFTER INSERT ON DeliveryItem
FOR EACH ROW
EXECUTE FUNCTION add_inventory_on_delivery();


--===========================================
-- Cardinality/Relationships

-- Store to Employee: 1:M (One store has many employees) //  

-- Store to Inventory: 1:M (One store manages inventory for many products) //  
 
-- Store to Sale: 1:M (One store processes many sales) //  

-- Store to Expense: 1:M (One store incurs many expenses) //  

-- Store to Delivery: 1:M (One store receives many deliveries) //  

-- Employee to ShiftSchedule: 1:M (One employee can have many shift records) // 

-- Department to Employee: 1:M (One department has many employees) //  

-- Category to Product: 1:M (One category includes many products) //  

-- Product to Inventory: 1:M (One product appears in inventory across many stores) // 

-- Product to ProductPricing: 1:M (One product has many pricing records over time) //  

-- Product to Promotion: 1:M (One product can be part of multiple promotions) // 

-- Product to SaleItem: 1:M (One product can appear in many sale transactions) //  

-- Product to ProductReturn: 1:M (One product can be returned in many transactions) //  

-- Product to DeliveryItem: 1:M (One product can appear in many deliveries) // 

-- Vendor to Delivery: 1:M (One vendor makes many deliveries) //  

-- Vendor to VendorProduct: 1:M (One vendor supplies multiple products) //  

-- VendorProduct (Vendor to Product): M:N via VendorProduct (A vendor supplies many products, and a product can come from multiple vendors) // 

-- Delivery to DeliveryItem: 1:M (One delivery includes many items) //  

-- Sale to SaleItem: 1:M (One sale contains multiple products) //  

-- Sale to ProductReturn: 1:M (One sale can include multiple returned products) // 

-- ReturnReason to ProductReturn: 1:M (One reason can apply to many returns) //  

-- SaleItem to Promotion: M:1 (Many sale items can reference one promotion) //  

-- Store to Manager (Employee): 1:1 (Each store is managed by one employee) //  
