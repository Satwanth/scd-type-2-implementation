# SCD Type 2 Implementation using Databricks & Delta Lake

## 📖 Project Overview
This project demonstrates a robust implementation of **Slowly Changing Dimensions (SCD) Type 2** using **Databricks** and **Delta Lake**. 

In Data Engineering, managing how data changes over time is critical. SCD Type 2 allows us to maintain a full history of changes by adding new records for updates while marking previous records as inactive. This ensures that historical reporting remains accurate even as dimension attributes change.

## 🛠️ Tech Stack
* **Platform:** Databricks with Delta Lake
* **Language:** SQL (Spark SQL)
* **Concepts:** Dimensional Modeling, ACID Transactions, History Preservation

---

## 🚀 Implementation Logic

### 1. The Scenario: Initial State vs. New Data
We begin with a `targeted_table` containing existing user data. We then receive a `staged_source` dataset containing updates and new records.

* **Existing Data (Target):** Contains users "Satwanth" (ID 1) and "Naidu" (ID 2).
* **Incoming Data (Source):** * **ID 1:** No change.
    * **ID 2 (Naidu):** City changed from "New Haven" to "Plano" (Update).
    * **ID 3 (Var):** New user in "Hartford" (Insert).

**Initial Target State:**
![Target Table](targeted_table%20Output.png)

**Incoming Source Data:**
![Source Data](staged_source%20Output.png)

---

### 2. The Solution: Step-by-Step
The implementation uses a 2-step approach to ensure ACID compliance using Delta Lake features.

#### Step A: Expire Old Records
We use the `MERGE` statement to identify records where the key (`id`) exists but attributes (`name` or `city`) have changed. We "expire" these records by updating the `end_time` to the current timestamp and setting `isActive` to `'N'`.

    MERGE INTO targeted_table t
    USING staged_source s
    ON t.id = s.id 
    WHEN MATCHED AND (t.name <> s.name OR t.city <> s.city) THEN
    UPDATE SET 
      t.end_time = current_timestamp(),
      t.isActive = 'N'

#### Step B: Insert New & Updated Records
We insert records that are either:
1.  **Entirely new:** ID does not exist in the target.
2.  **New versions of updated records:** The ID exists, but the attributes have changed.

These new records are inserted with `start_time` as the current timestamp and `end_time` as a high watermark (`9999-09-09`) to indicate they are currently active.

    INSERT INTO targeted_table 
    SELECT 
        s.id, 
        s.name, 
        s.city, 
        current_timestamp() as start_date, 
        '9999-09-09 00:00:00' as end_date, 
        'Y' as isActive 
    FROM staged_source s 
    LEFT JOIN targeted_table t 
    ON s.id = t.id 
    WHERE t.id IS NULL OR (t.name <> s.name OR t.city <> s.city)

---

## 📊 Final Results
After running the pipeline, the table accurately reflects the history:

1.  **Satwanth (ID 1):** Remains unchanged.
2.  **Naidu (ID 2):** * The old record (New Haven) is preserved but marked **Inactive** (`N`).
    * The new record (Plano) is inserted and marked **Active** (`Y`).
3.  **Var (ID 3):** Newly inserted as **Active** (`Y`).

![Final Output](Final%20Output.png)

---

## 📂 Repository Structure
* `SCD Type 2 Implementation.ipynb`: The Databricks notebook containing the full source code.
* `targeted_table Output.png`: Screenshot of the initial target table state.
* `staged_source Output.png`: Screenshot of the incoming staging data.
* `Final Output.png`: Screenshot of the final table with history preserved.

## 💻 How to Run
1.  Import the `.ipynb` file into your Databricks Workspace.
2.  Run the cells sequentially.
3.  The notebook will create a database `scdtp2db`, setup the tables, and execute the SCD logic.
