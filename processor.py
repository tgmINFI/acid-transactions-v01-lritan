import sqlite3

class ShipmentProcessor:
    def __init__(self, db_path):
        self.db_path = db_path

    def process_shipment(self, item_name, quantity, log_callback):
        """
        Executes the shipment logic.
        :param item_name: Name of the item
        :param quantity: Amount to move
        :param log_callback: A function to print to the GUI console
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        log_callback(f"--- STARTING TRANSACTION: Move {quantity} of {item_name} ---")

        # ==============================================================================
        # STUDENT TODO: Fix the Transaction Logic below.
        # Currently, if Step 1 fails, Step 2 still runs, creating a "Ghost Shipment".
        # Ensure the operation is ATOMIC (All or Nothing).
        # ==============================================================================

        try:
            # STEP 1: Update Inventory
            # This will raise sqlite3.IntegrityError if stock becomes negative
            cursor.execute("UPDATE inventory SET stock_qty = stock_qty - ? WHERE item_name = ?", 
                        (quantity, item_name))
            log_callback(">> STEP 1 SUCCESS: Inventory Deducted.")

        except sqlite3.IntegrityError as e:
            log_callback(f">> STEP 1 FAILED: {e}") 
            # NEU
            cursor.execute("SELECT stock_qty FROM inventory WHERE item_name = ?", (item_name,))
            current_stock = cursor.fetchone()[0] 
            if current_stock <= 0:
                log_callback("}\n >> Das Produkt ist ausverkauft!")
            else:
                log_callback(f"\n >> Es sind nur {current_stock} {item_name} auf Lager! ")
            conn.rollback()
            conn.close()
            return
        try:
            # STEP 2: Log the Shipment
            cursor.execute("INSERT INTO shipment_log (item_name, qty_moved) VALUES (?, ?)", 
                        (item_name, quantity))
            log_callback(">> STEP 2 SUCCESS: Shipment Logged.")
        
        except Exception as e:
            log_callback(f">> STEP 2 FAILED: {e}")

        # Final Commit
        conn.commit()
        log_callback("--- TRANSACTION COMMITTED ---")
        
        conn.close()