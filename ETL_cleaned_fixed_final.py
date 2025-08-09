import pandas as pd
import psycopg2


df_sales = pd.read_csv('Sales_Master.csv')
print(df_sales.head())
df_sales.info()

df_expense = pd.read_csv('Expense_Master.csv')
print(df_expense.head())
df_expense.info()

df_delivery = pd.read_csv('Delivery_Master.csv')
print(df_delivery.head())
df_delivery.info()

df_shift = pd.read_csv('Shift_Master.csv')
print(df_shift.head())
df_shift.info()

# Connect to an existing database 
conn = psycopg2.connect("dbname=ABCFoodmart user=postgres host=localhost password=123")

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a command: this creates all table in the database 
cur.execute("""
                
    CREATE TABLE IF NOT EXISTS Store (
        store_id SERIAL PRIMARY KEY,
        address VARCHAR(255) NOT NULL,
        city VARCHAR(100) NOT NULL,
        state VARCHAR(50) NOT NULL,
        zipcode VARCHAR(10) NOT NULL,
        operating_hours VARCHAR(100) NOT NULL,
        manager_id INTEGER UNIQUE
    );


    CREATE TABLE IF NOT EXISTS Department (
        department_id SERIAL PRIMARY KEY,
        department_name VARCHAR(100) NOT NULL
    );


    CREATE TABLE IF NOT EXISTS Employee (
        employee_id SERIAL PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        email VARCHAR(100) NOT NULL UNIQUE,
        phone VARCHAR(20) NOT NULL,
        role VARCHAR(50) NOT NULL,
        store_id INTEGER REFERENCES Store(store_id) ON DELETE CASCADE,
        department_id INTEGER REFERENCES Department(department_id)
    );



    CREATE TABLE IF NOT EXISTS ShiftSchedule (
        schedule_id SERIAL PRIMARY KEY,
        employee_id INTEGER REFERENCES Employee(employee_id) ON DELETE CASCADE,
        shift_date DATE NOT NULL,
        start_time TIME NOT NULL,
        end_time TIME NOT NULL
    );



    CREATE TABLE IF NOT EXISTS Category (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(100) NOT NULL
    );


    CREATE TABLE IF NOT EXISTS Product (
        sku varchar(20) PRIMARY KEY,
        product_name VARCHAR(150) NOT NULL,
        brand VARCHAR(100) NOT NULL,
        shelf_location VARCHAR(50) NOT NULL,
        category_id INTEGER REFERENCES Category(category_id)
    );


    CREATE TABLE IF NOT EXISTS ProductPricing (
        sku varchar(20) REFERENCES Product(sku),
        price_date DATE NOT NULL,
        regular_price NUMERIC(10,2) NOT NULL,
        promo_price NUMERIC(10,2),
        PRIMARY KEY (sku, price_date)
    );


    CREATE TABLE IF NOT EXISTS Inventory (
        inventory_id SERIAL PRIMARY KEY,
        store_id INTEGER REFERENCES Store(store_id),
        sku varchar(20) REFERENCES Product(sku),
        quantity_on_hand INTEGER NOT NULL,
        reorder_threshold INTEGER NOT NULL,
        restock_status VARCHAR(20) CHECK (restock_status IN ('Restock Needed', 'In Stock')) NOT NULL,
        UNIQUE (store_id, sku)
    );


    CREATE TABLE IF NOT EXISTS Vendor (
    vendor_id SERIAL PRIMARY KEY,
    vendor_name VARCHAR(100) NOT NULL,
    vendor_tier VARCHAR(10) NOT NULL
    );


    CREATE TABLE IF NOT EXISTS VendorProduct (
        vendor_id INTEGER REFERENCES Vendor(vendor_id) ON DELETE CASCADE,
        sku varchar(20) REFERENCES Product(sku),
        PRIMARY KEY (vendor_id, sku)
    );


    CREATE TABLE IF NOT EXISTS Delivery (
        delivery_id SERIAL PRIMARY KEY,
        vendor_id INTEGER REFERENCES Vendor(vendor_id),
        store_id INTEGER REFERENCES Store(store_id),
        delivery_date DATE NOT NULL,
        status VARCHAR(20) CHECK (status IN ('Completed', 'Delayed')) NOT NULL
    );


    CREATE TABLE IF NOT EXISTS DeliveryItem (
        delivery_id INTEGER REFERENCES Delivery(delivery_id) ON DELETE CASCADE,
        sku varchar(20) REFERENCES Product(sku),
        quantity INTEGER NOT NULL,
        PRIMARY KEY (delivery_id, sku)
    );



    CREATE TABLE IF NOT EXISTS Promotion (
        promo_id SERIAL PRIMARY KEY,
        sku varchar(20) REFERENCES Product(sku),
        start_date DATE,
        end_date DATE,
        discount_amount NUMERIC
    );


    CREATE TABLE IF NOT EXISTS Sale (
        sale_id SERIAL PRIMARY KEY,
        store_id INTEGER REFERENCES Store(store_id),
        sale_datetime TIMESTAMP,
        payment_type VARCHAR(20) CHECK (payment_type IN ('Cash', 'Credit Card', 'Mobile'))
    );

                
    CREATE TABLE IF NOT EXISTS SaleItem (
        sale_id INTEGER REFERENCES Sale(sale_id) ON DELETE CASCADE,
        sku varchar(20) REFERENCES Product(sku),
        quantity_sold INTEGER NOT NULL,
        unit_price NUMERIC(10,2) NOT NULL,
        promo_applied BOOLEAN DEFAULT FALSE,
        promo_discount NUMERIC(10,2),
        promo_id INTEGER REFERENCES Promotion(promo_id),
        PRIMARY KEY (sale_id, sku)
    );



    CREATE TABLE IF NOT EXISTS Expense (
        expense_id SERIAL PRIMARY KEY,
        store_id INTEGER REFERENCES Store(store_id),
        expense_date DATE NOT NULL,
        expense_category VARCHAR(50) CHECK (expense_category IN ('Wages', 'Utilities', 'Spoilage', 'Other')) NOT NULL,
        amount NUMERIC(12,2) NOT NULL
    );


    CREATE TABLE IF NOT EXISTS ReturnReason (
        reason_code VARCHAR(10) PRIMARY KEY,
        description TEXT
    );


    CREATE TABLE IF NOT EXISTS ProductReturn (
        return_id SERIAL PRIMARY KEY,
        sale_id INTEGER REFERENCES Sale(sale_id),
        sku varchar(20) REFERENCES Product(sku),
        return_date DATE NOT NULL,
        quantity_returned INTEGER NOT NULL,
        reason_code VARCHAR(10) REFERENCES ReturnReason(reason_code)
    );

DROP TRIGGER IF EXISTS trg_update_restock_status ON Inventory;
DROP FUNCTION IF EXISTS update_restock_status() CASCADE;

    CREATE OR REPLACE FUNCTION update_restock_status()
    RETURNS TRIGGER AS $$
    BEGIN
        IF NEW.quantity_on_hand <= NEW.reorder_threshold THEN
            NEW.restock_status := 'Restock Needed';
        ELSE
            NEW.restock_status := 'In Stock';
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

            

    CREATE TRIGGER trg_update_restock_status
    BEFORE INSERT OR UPDATE ON Inventory
    FOR EACH ROW
    EXECUTE FUNCTION update_restock_status();


DROP TRIGGER IF EXISTS trg_deduct_inventory ON SaleItem;
DROP FUNCTION IF EXISTS deduct_inventory_after_sale() CASCADE;

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


    CREATE TRIGGER trg_deduct_inventory
    AFTER INSERT ON SaleItem
    FOR EACH ROW
    EXECUTE FUNCTION deduct_inventory_after_sale();


DROP TRIGGER IF EXISTS trg_add_inventory_on_return ON ProductReturn;
DROP FUNCTION IF EXISTS add_inventory_on_return() CASCADE;

    CREATE OR REPLACE FUNCTION  add_inventory_on_return()
    RETURNS TRIGGER AS $$
    BEGIN
    UPDATE Inventory
    SET quantity_on_hand = quantity_on_hand + NEW.quantity_returned
    WHERE store_id = (SELECT store_id FROM Sale WHERE sale_id = NEW.sale_id)
        AND sku = NEW.sku;
    RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;


    CREATE TRIGGER trg_add_inventory_on_return
    AFTER INSERT ON ProductReturn
    FOR EACH ROW
    EXECUTE FUNCTION add_inventory_on_return();


DROP TRIGGER IF EXISTS trg_add_inventory_on_delivery ON DeliveryItem;
DROP FUNCTION IF EXISTS add_inventory_on_delivery() CASCADE;
            
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

            
""")

conn.commit()


# Create Store table from df_sales
store_df = df_sales[['store_id', 'address', 'city', 'state', 'zipcode', 'operating_hours']].drop_duplicates()
store_df.columns = ['store_id', 'address', 'city', 'state', 'zipcode', 'operating_hours']


# Insert into Store table
inserted = 0
for idx, row in store_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO Store (store_id, address, city, state, zipcode, operating_hours)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (store_id) DO NOTHING;
        """, (
            int(row['store_id']),
            row['address'],
            row['city'],
            row['state'],
            str(row['zipcode']), 
            row['operating_hours'])
        )
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error inserting row {idx}: {e}")


conn.commit()
print(f"✅ {inserted} rows actually inserted into Store table.")




# Create Department table from df_shift
dept_df = df_shift[['department_id', 'department_name']].drop_duplicates()
dept_df.columns = ['department_id', 'department_name']

# Insert into Store table into Department
inserted = 0
for idx, row in dept_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO Department (department_id, department_name)
            VALUES (%s, %s)
            ON CONFLICT (department_id) DO NOTHING;
        """, (
            int(row['department_id']),
            row['department_name']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error inserting row {idx}: {e}")

# Finalize transaction and report inserted rows
conn.commit()
print(f"✅ {inserted} rows actually inserted into Department table.")


# Create Employee table from df_shift
emp_df = df_shift[[
    'employee_id',
    'first_name',
    'last_name',
    'email',
    'phone',
    'role',
    'store_id',
    'department_id'
]].drop_duplicates()

# 4) INSERT 
inserted = 0
for idx, row in emp_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO Employee (
                employee_id,
                first_name,
                last_name,
                email,
                phone,
                role,
                store_id,
                department_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (employee_id) DO NOTHING;
        """, (
            int(row['employee_id']),
            row['first_name'],
            row['last_name'],
            row['email'],
            row['phone'],
            row['role'],
            int(row['store_id']),
            int(row['department_id'])
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error inserting employee row {idx}: {e}")


conn.commit()
print(f"✅ Inserted {inserted} rows into Employee table.")


# Update Store.manager_id based on Employee role
cur.execute("""
    UPDATE Store
    SET manager_id = e.employee_id
    FROM Employee e
    WHERE e.role = 'Store Manager'
      AND Store.store_id = e.store_id;
""")
conn.commit()
print("✅ Store table manager_id is updated.")



# Create ShiftSchedule table from df_shift

shift_schedule_df = df_shift[
    ['schedule_id', 'employee_id', 'shift_date', 'start_time', 'end_time']
].drop_duplicates()

inserted = 0
for idx, row in shift_schedule_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO ShiftSchedule
                (schedule_id, employee_id, shift_date, start_time, end_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (schedule_id) DO NOTHING;
        """, (
            int(row['schedule_id']),
            int(row['employee_id']),
            row['shift_date'],
            row['start_time'],
            row['end_time']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ ShiftSchedule errr at row {idx}: {e}")


conn.commit()
print(f"✅ ShiftSchedule: Total {inserted} rows inserted.")

# Create Category table from df_sales

cat_df = df_sales[['category_id', 'category_name']].drop_duplicates()


cat_df['category_id'] = cat_df['category_id'].astype(int)
cat_df['category_name'] = cat_df['category_name'].astype(str)


inserted = 0
for idx, row in cat_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO Category (category_id, category_name)
            VALUES (%s, %s)
            ON CONFLICT (category_id) DO NOTHING;
        """, (
            row['category_id'],
            row['category_name']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Category insert error at row {idx}: {e}")

conn.commit()
print(f"✅ Category: Total {inserted} rows inserted.")


# Create Product table from df_sales

prod_df = df_sales[[
    'sku',
    'product_name',
    'brand',
    'shelf_location',
    'category_id'
]].drop_duplicates()

prod_df['sku']            = prod_df['sku'].astype(str)
prod_df['product_name']   = prod_df['product_name'].astype(str)
prod_df['brand']          = prod_df['brand'].astype(str)
prod_df['shelf_location'] = prod_df['shelf_location'].astype(str)
prod_df['category_id']    = prod_df['category_id'].astype(int)


inserted = 0
for idx, row in prod_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO Product
                (sku, product_name, brand, shelf_location, category_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sku) DO NOTHING;
        """, (
            row['sku'],
            row['product_name'],
            row['brand'],
            row['shelf_location'],
            row['category_id']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Product insert error at row {idx}: {e}")

conn.commit()
print(f"✅ Product: total {inserted} rows inserted.")


# Create ProductPricing table from df_sales
pricing_df = df_sales[['sku', 'price_date', 'regular_price', 'promo_price']].drop_duplicates()

pricing_df['regular_price'] = pricing_df['regular_price'].astype(float)
pricing_df['promo_price']   = pricing_df['promo_price'].astype(float)
pricing_df['promo_price']   = pricing_df['promo_price'].where(pricing_df['promo_price'].notna(), None)

inserted = 0
for _, row in pricing_df.iterrows():
    cur.execute("""
        INSERT INTO ProductPricing
            (sku, price_date, regular_price, promo_price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (sku, price_date) DO NOTHING;
    """, (
        row['sku'],
        row['price_date'],
        row['regular_price'],
        row['promo_price']   
    ))
    if cur.rowcount == 1:
            if cur.rowcount == 1:
                inserted += 1

conn.commit()
print(f"✅ ProductPricing: actually inserted {inserted} new rows.")



# Create Inventory table from df_sales
inv_df = df_sales[[
    'inventory_id', 'store_id', 'sku', 'quantity_on_hand', 'reorder_threshold'
]].dropna()

inv_df = inv_df.drop_duplicates(subset=['store_id', 'sku'])

inv_df['inventory_id'] = inv_df['inventory_id'].astype(int)
inv_df['store_id'] = inv_df['store_id'].astype(int)
inv_df['sku'] = inv_df['sku'].astype(str)
inv_df['quantity_on_hand'] = inv_df['quantity_on_hand'].astype(int)
inv_df['reorder_threshold'] = inv_df['reorder_threshold'].astype(int)

inserted = 0
for idx, row in inv_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO Inventory (
                inventory_id, store_id, sku, quantity_on_hand, reorder_threshold
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (store_id, sku) DO NOTHING;
        """, (
            row['inventory_id'],
            row['store_id'],
            row['sku'],
            row['quantity_on_hand'],
            row['reorder_threshold']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Inventory insert error at row {idx}: {e}")

conn.commit()
print(f"✅ Inventory: total {inserted} rows inserted.")



# Create Vendor table from df_sales 
# Drop duplicates to get unique vendors
vendor_df = df_sales[['primary_vendor_id', 'vendor_name', 'vendor_tier']] \
    .dropna(subset=['primary_vendor_id', 'vendor_name', 'vendor_tier']) \
    .drop_duplicates().sort_values(by='primary_vendor_id')

# Rename columns to match Vendor table
vendor_df.columns = ['vendor_id', 'vendor_name', 'vendor_tier']


# Insert rows into Vendor table
inserted = 0
for idx, row in vendor_df.iterrows():
    try:
        cur.execute("""
            WITH ins AS (
                INSERT INTO Vendor (vendor_id, vendor_name, vendor_tier)
                VALUES (%s, %s, %s)
                ON CONFLICT (vendor_id) DO NOTHING
                RETURNING vendor_id
            )
            SELECT COUNT(*) FROM ins;
        """, (
            int(row['vendor_id']),
            row['vendor_name'],
            row['vendor_tier']
        ))

        # Fetch result of SELECT COUNT(*) to see if insert actually happened
        result = cur.fetchone()[0]
        inserted += result

    except Exception as e:
        print(f"❌ Error inserting vendor row {idx}: {e}")

conn.commit()
print(f"✅ Actually inserted {inserted} new rows into Vendor table.")

# Create VendorProduct table from df_sales

# Extract and clean Vendor-Product relationships
vendor_product_df = df_sales[['primary_vendor_id', 'sku']].drop_duplicates().dropna()
vendor_product_df.columns = ['vendor_id', 'sku']
vendor_product_df['vendor_id'] = vendor_product_df['vendor_id'].astype(int)
vendor_product_df['sku'] = vendor_product_df['sku'].astype(str)

inserted = 0
for idx, row in vendor_product_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO VendorProduct (vendor_id, sku)
            VALUES (%s, %s)
            ON CONFLICT (vendor_id, sku) DO NOTHING;
        """, (
            row['vendor_id'],
            row['sku']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error inserting row {idx}: {e}")

conn.commit()
print(f"✅ {inserted} rows actually inserted into VendorProduct table.")

# Create Delivery table from df_delivery 
delivery_unique_df = df_delivery[['delivery_id', 'vendor_id', 'store_id', 'delivery_date', 'status']].drop_duplicates()


# INSERT
inserted = 0
for idx, row in delivery_unique_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO Delivery (delivery_id, vendor_id, store_id, delivery_date, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (delivery_id) DO NOTHING;
        """, (
            int(row['delivery_id']),
            int(row['vendor_id']),
            int(row['store_id']),
            row['delivery_date'],
            row['status']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error inserting delivery row {idx}: {e}")

conn.commit()
print(f"✅ {inserted} rows actually inserted into Delivery table.") 



# Create DeliveryItem table from df_delivery

delivery_item_df = df_delivery[['delivery_id', 'sku', 'delivered_quantity']].drop_duplicates()
delivery_item_df.columns = ['delivery_id', 'sku', 'quantity']


# # Insert into Store table into DeliveryItem table
inserted = 0
for idx, row in delivery_item_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO DeliveryItem (delivery_id, sku, quantity)
            VALUES (%s, %s, %s)
            ON CONFLICT (delivery_id, sku) DO NOTHING;
        """, (
            int(row['delivery_id']),
            row['sku'],
            int(row['quantity'])
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error inserting row {idx}: {e}")

conn.commit()
print(f"✅ DeliveryItem: {inserted} rows inserted.")


# Create Promotion table from df_sales

# Filter and clean promotion columns
promotion_df = df_sales[['promo_id', 'sku', 'start_date', 'end_date', 'discount_amount']]\
    .dropna(subset=['promo_id', 'sku'])\
    .drop_duplicates()

# Convert column types
promotion_df['promo_id'] = promotion_df['promo_id'].astype(int)
promotion_df['sku'] = promotion_df['sku'].astype(str)
promotion_df['discount_amount'] = promotion_df['discount_amount'].astype(float)

# Insert into Store table into Promotion table
inserted = 0
for idx, row in promotion_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO Promotion (promo_id, sku, start_date, end_date, discount_amount)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (promo_id) DO NOTHING;
        """, (
            int(row['promo_id']),
            row['sku'],
            row['start_date'],
            row['end_date'],
            row['discount_amount']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error at row {idx}: {e}")

conn.commit()
print(f"✅ {inserted} rows actually inserted into Promotion table.")


# Create Sale table from df_sales 

sale_df = df_sales[['sale_id', 'store_id', 'sale_datetime', 'payment_type']].drop_duplicates()
sale_df['sale_id'] = sale_df['sale_id'].astype(int)
sale_df['store_id'] = sale_df['store_id'].astype(int)
sale_df['payment_type'] = sale_df['payment_type'].astype(str)


inserted = 0
for idx, row in sale_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO Sale (
                sale_id,
                store_id,
                sale_datetime,
                payment_type
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (sale_id) DO NOTHING;
        """, (
            row['sale_id'],
            row['store_id'],
            row['sale_datetime'],
            row['payment_type']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Sale insert error at row {idx}: {e}")

conn.commit()
print(f"✅ {inserted} rows actually inserted into Sale table.")



# Create SaleItem table from df_sales
sale_item_df = df_sales[[
    'sale_id',
    'sku',
    'quantity_sold',
    'unit_price',
    'promo_applied',
    'promo_discount',
    'promo_id'
]].drop_duplicates()

# Convert column types for SaleItem
sale_item_df['sale_id'] = sale_item_df['sale_id'].astype(int)
sale_item_df['sku'] = sale_item_df['sku'].astype(str)
sale_item_df['quantity_sold'] = sale_item_df['quantity_sold'].astype(int)
sale_item_df['unit_price'] = sale_item_df['unit_price'].astype(float)
sale_item_df['promo_applied'] = sale_item_df['promo_applied'].astype(bool)
sale_item_df['promo_discount'] = sale_item_df['promo_discount'].astype(float)

sale_item_df['promo_id'] = sale_item_df['promo_id'].apply(lambda x: int(x) if pd.notnull(x) else None)

# Insert rows into SaleItem table
inserted = 0
for idx, row in sale_item_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO SaleItem (sale_id, sku, quantity_sold, unit_price, promo_applied, promo_discount, promo_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row['sale_id']),
            row['sku'],
            int(row['quantity_sold']),
            float(row['unit_price']),
            row['promo_applied'],
            float(row['promo_discount']) if pd.notnull(row['promo_discount']) else None,
            int(row['promo_id']) if pd.notnull(row['promo_id']) else None
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ SaleItem insert error at row {idx}: {e}")

conn.commit()
print(f"✅ {inserted} rows successfully inserted into SaleItem table.")


# Create Expense table from df_expense 
expense_df = df_expense[['store_id', 'expense_date', 'expense_category', 'amount']].drop_duplicates()


# 1. 필드 정리 및 타입 지정
df_expense['store_id'] = df_expense['store_id'].astype(int)
df_expense['expense_category'] = df_expense['expense_category'].astype(str)
df_expense['amount'] = df_expense['amount'].astype(float)

# 2. Insert rows into Expense table
inserted = 0
for idx, row in df_expense.iterrows():
    try:
        cur.execute("""
            INSERT INTO Expense (store_id, expense_date, expense_category, amount)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (
            row['store_id'],
            row['expense_date'],
            row['expense_category'],
            row['amount']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error inserting expense row {idx}: {e}")

conn.commit()
print(f"✅ {inserted} rows actually inserted into Expense table.")


# Create ReturnReason table from df_sales
returnreason_df = df_sales[['reason_code','description']]\
    .dropna(subset=['reason_code', 'description'])\
    .drop_duplicates()

inserted = 0
for _, row in returnreason_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO ReturnReason (reason_code, description)
            VALUES (%s, %s)
            ON CONFLICT (reason_code) DO NOTHING;
        """, (
            row['reason_code'],
            row['description']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error inserting reason {row['reason_code']}: {e}")

conn.commit()
print(f"✅ {inserted} rows actually inserted into ReturnReason table.")

# Create ProductReturn table from df_sales

productreturn_df = df_sales[df_sales['return_exists'] == True][[
    'return_id', 'sale_id', 'sku', 'return_date', 'quantity_returned', 'reason_code'
]].dropna(subset=['return_id', 'sale_id', 'sku', 'return_date', 'quantity_returned', 'reason_code'])

productreturn_df['return_id'] = productreturn_df['return_id'].astype(int)
productreturn_df['sale_id'] = productreturn_df['sale_id'].astype(int)
productreturn_df['sku'] = productreturn_df['sku'].astype(str)
productreturn_df['quantity_returned'] = productreturn_df['quantity_returned'].astype(int)
productreturn_df['reason_code'] = productreturn_df['reason_code'].astype(str)


inserted = 0
for idx, row in productreturn_df.iterrows():
    try:
        cur.execute("""
            INSERT INTO ProductReturn (return_id, sale_id, sku, return_date, quantity_returned, reason_code)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (return_id) DO NOTHING;
        """, (
            row['return_id'],
            row['sale_id'],
            row['sku'],
            row['return_date'],
            row['quantity_returned'],
            row['reason_code']
        ))
        if cur.rowcount == 1:
            inserted += 1
    except Exception as e:
        print(f"❌ Error inserting return_id {row['return_id']}: {e}")

conn.commit()
print(f"✅ {inserted} rows actually inserted into ProductReturn table.")

conn.commit()

cur.close()
conn.close()
