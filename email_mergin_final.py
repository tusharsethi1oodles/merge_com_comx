import pandas as pd
import mysql.connector
from mysql.connector import Error
from difflib import SequenceMatcher
import json
from datetime import datetime

PROPERTY_IDS = [
    "former_last_name", "notes", "ppsn_document_type", "photo_url", "pronounced", "signature_attachment",
    "crm_id", "exchange_ref_id", "import_people_name", "leads_transactions_id", "status_id", "industry_id","email","is_delete"
]

def log(msg):
    with open("property_update_log_night_two_ph.txt", "a") as f:
        f.write(msg + "\n")
    # print(msg)

def connect_db(db_name):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=db_name
    )


def similarity_ratio(a, b):
    return int(SequenceMatcher(None, a, b).ratio() * 100)

def update_entity_mapping_type2(conn_entity, correct_id, incorrect_id):
    try:
        cursor=conn_entity.cursor(buffered=True)
        cursor.execute("SELECT parent_id FROM entity_mapping WHERE entity_id = %s", (correct_id,))
        correct_parents = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT parent_id FROM entity_mapping WHERE entity_id = %s", (incorrect_id,))
        incorrect_parents = {row[0] for row in cursor.fetchall()}

        duplicates = correct_parents & incorrect_parents
        if duplicates:
            cursor.execute("""
                DELETE FROM entity_mapping
                WHERE entity_id = %s AND parent_id IN (%s)
            """ % ("%s", ",".join(map(str, duplicates))), (incorrect_id,))
            log(f"🗑️ Hard-deleted duplicate parent links for entity_id {incorrect_id}")

        to_update = incorrect_parents - correct_parents
        if to_update:
            cursor.execute("""
                UPDATE entity_mapping
                SET entity_id = %s
                WHERE entity_id = %s AND parent_id IN (%s)
            """ % ("%s", "%s", ",".join(map(str, to_update))), (correct_id, incorrect_id))
            log(f"🔁 Updated entity_id to {correct_id} for parent_ids previously linked to {incorrect_id}")

        conn_entity.commit()
        log("💾 Commit successful for entity_mapping type 2")

    except Exception as e:
        log(f"❌ Error in update_entity_mapping_type2: {e}")



def update_entity_mapping_type1(conn_entity, correct_id, incorrect_id):
    try:
        cursor=conn_entity.cursor(buffered=True)

        cursor.execute("SELECT entity_id FROM entity_mapping WHERE parent_id = %s", (correct_id,))
        correct_children = {row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT entity_id FROM entity_mapping WHERE parent_id = %s", (incorrect_id,))
        incorrect_children = {row[0] for row in cursor.fetchall()}

        duplicates = correct_children & incorrect_children
        if duplicates:
            cursor.execute("""
                DELETE FROM entity_mapping
                WHERE parent_id = %s AND entity_id IN (%s)
            """ % ("%s", ",".join(map(str, duplicates))), (incorrect_id,))
            log(f"🗑️ Hard-deleted duplicate child links for parent_id {incorrect_id}")

        to_update = incorrect_children - correct_children
        if to_update:
            cursor.execute("""
                UPDATE entity_mapping
                SET parent_id = %s
                WHERE parent_id = %s AND entity_id IN (%s)
            """ % ("%s", "%s", ",".join(map(str, to_update))), (correct_id, incorrect_id))
            log(f"🔁 Updated parent_id to {correct_id} for entity_ids previously linked to {incorrect_id}")

        conn_entity.commit()
        log("💾 Commit successful for entity_mapping type 1")

    except Exception as e:
        log(f"❌ Error in update_entity_mapping_type1: {e}")

def load_data(csv_path):
    df = pd.read_csv(csv_path)
    return df

import json



def update_leads_transaction_ids(master_id, other_id, conn_other, conn_entity, entitiy_type,json_path,email,cnt):
    try:
        # conn_other, conn_entity
        cursor_dms=conn_other.cursor(buffered=True)
        cursor_en=conn_entity.cursor(buffered=True)

        master_people_entitiy_id=master_id
        updateFields = [
            "deal_fields", "deal_fields_values", "deal_sum", "leads_connections",
            "leads_notes", "leads_sss", "leads_tags", "leads_tickets",
            "leads_transactions_contacts", "lock_notes"
        ]

        if entitiy_type == 1 or entitiy_type == 2:
            cursor_en.execute("""
                SELECT property_value FROM entity_property 
                WHERE entity_id = %s AND property_id = 'leads_transactions_id'
            """, (master_people_entitiy_id,))
            row = cursor_en.fetchone()
            if not row:
                log(f"⚠️ master_id {master_people_entitiy_id} not found in entity_property ")
                return
            result = row[0]

            cursor_en.execute("""
                SELECT property_value FROM entity_property 
                WHERE entity_id = %s AND property_id = 'leads_transactions_id'
            """, (other_id,))
            row_other = cursor_en.fetchone()

            if not row_other:
                log(f"⚠️ other_id {other_id} not found in entity_property")
                return

            other_ids_str = row_other[0]
        else:
            log(f"❌ Invalid type: {entitiy_type}. Must be 1 or 2.")
            return

        if not result:
            log(f"⚠️ ID {master_people_entitiy_id} not found in type {entitiy_type}")
            return

        master_leads_transaction_id = result
        if not master_leads_transaction_id or master_leads_transaction_id == 0:
            log(f"⚠️ ID {master_people_entitiy_id} has no valid leads_transactions_id -- email is {email} and cnt {cnt}")
            return

        cursor_dms.execute("SELECT id FROM leads_transactions WHERE id = %s", (master_leads_transaction_id,))
        if not cursor_dms.fetchone():
            log(f"⚠️ leads_transactions_id {master_leads_transaction_id} does not exist in leads_transactions")
            return
        
        log(f"in update leads func {cnt} and email {email} and correct entity id {master_id}")

        for table in updateFields:
            if master_leads_transaction_id != 0:
                query = f"""
                    UPDATE {table}
                    SET leads_transactions_id = %s
                    WHERE leads_transactions_id =%s
                """
                cursor_dms.execute(query, (master_leads_transaction_id,other_ids_str))
                log(f"✅ Updated leads_transactions_id in `{table}`")

            cursor_dms.execute(f"""
                UPDATE key_library
                SET lead_transcation_id = %s
                WHERE lead_transcation_id = %s
            """, (master_leads_transaction_id,other_ids_str))
            log("✅ Updated key_library")

            cursor_dms.execute(f"""
                SELECT SUM(investment_hours) FROM investment_hours
                WHERE leads_transactions_id IN ({other_ids_str}, {master_leads_transaction_id})
            """)
            total_hours = cursor_dms.fetchone()[0] or 0

            cursor_dms.execute("""
                UPDATE investment_hours
                SET investment_hours = %s
                WHERE leads_transactions_id = %s
            """, (total_hours, master_leads_transaction_id))
            log("✅ Updated investment_hours total")

            if other_ids_str!=0:
                cursor_dms.execute(f"""
                    DELETE FROM investment_hours
                    WHERE leads_transactions_id IN ({other_ids_str})
                """)

        conn_other.commit()
        log("💾 Commit successful")

    except Exception as e:
        log(f"❌ Error in update_leads_transaction_ids: {e} {email} {cnt}")

def update_entity_role_links(conn_en, correct_id, incorrect_id):
    try:
        cursor=conn_en.cursor(buffered=True)

        cursor.execute("SELECT role_id FROM entity_role WHERE entity_id = %s", (correct_id,))
        correct_roles = {row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT role_id FROM entity_role WHERE entity_id = %s", (incorrect_id,))
        incorrect_roles = {row[0] for row in cursor.fetchall()}

        # Delete duplicate roles that exist in both correct and incorrect
        common_roles = correct_roles & incorrect_roles
        if common_roles:
            query = f"""
                DELETE FROM entity_role
                WHERE entity_id = %s AND role_id IN ({','.join(['%s'] * len(common_roles))})
            """
            cursor.execute(query, (incorrect_id, *common_roles))
            log(f"🗑️ Deleted duplicate roles for incorrect_id {incorrect_id} that already exist in correct_id {correct_id}")

        # Move remaining unique roles from incorrect to correct
        roles_to_update = incorrect_roles - correct_roles
        if roles_to_update:
            query = f"""
                UPDATE entity_role
                SET entity_id = %s
                WHERE entity_id = %s AND role_id IN ({','.join(['%s'] * len(roles_to_update))})
            """
            cursor.execute(query, (correct_id, incorrect_id, *roles_to_update))
            log(f"🔁 Moved unique roles from {incorrect_id} to {correct_id}")


        conn_en.commit()

    except Exception as e:
        log(f"❌ Error in update_entity_role_links for {correct_id}: {e}")


def update_properties_and_phone(csv_path):
    df = load_data(csv_path)
    df = df[df['incorrect_type'] == df['correct_type']]

    conn_entity = connect_db("entities_19april")
    conn_other = connect_db("dms_dump_19april")

    cursor_entity = conn_entity.cursor(buffered=True)
    cursor_dms = conn_other.cursor(buffered=True)
    cnt=0
    for _, row in df.iterrows():
        cnt+=1
        incorrect_id = int(row['incorrect_entity_id'])
        correct_id = int(row['correct_entity_id'])
        entity_type = int(row['correct_type'])

        correct_email=str(row['correct_email'])
        print(f"{cnt} -- correct email -- {correct_email}")

        correct_phone = str(row['correct_phone_number']).strip() if pd.notna(row['correct_phone_number']) else ''
        incorrect_phone = str(row['incorrect_phone_number']).strip() if pd.notna(row['incorrect_phone_number']) else ''

        try:
            if not correct_phone and incorrect_phone:
                cursor_entity.execute("""
                    UPDATE entity_property
                    SET entity_id = %s
                    WHERE entity_id = %s AND property_id = 'phone_number'
                """, (correct_id, incorrect_id))
                conn_entity.commit()   
                log(f"✅ Phone entity_id replaced: {incorrect_id} → {correct_id}")
            elif correct_phone and incorrect_phone:
                if similarity_ratio(correct_phone, incorrect_phone) < 60:
                    cursor_entity.execute("""
                        UPDATE entity_property
                        SET entity_id = %s
                        WHERE entity_id = %s AND property_id = 'phone_number'
                    """, (correct_id, incorrect_id))
                    conn_entity.commit()
                    log(f"✅ Phone entity_id replaced (low similarity): {incorrect_id} → {correct_id} -- > {correct_phone} and {incorrect_phone}")

        except Exception as e:
            log(f"❌ Phone update failed for {incorrect_id}: {e}")
        # ----------------------------------------------------------------------------------- # 
        if entity_type == 1:
            json_path = "records_entity_mapping_for_global_organisations_all_now.json"
            update_leads_transaction_ids(correct_id, incorrect_id, conn_other, conn_entity,entity_type, json_path,correct_email,cnt)
        elif entity_type == 2:
            json_path = 'records_entity_mapping_for_people.json'
            update_leads_transaction_ids(correct_id, incorrect_id, conn_other,conn_entity, entity_type, json_path,correct_email,cnt)
        # ----------------------------------------------------------------------------------- # 
        for property_id in PROPERTY_IDS:
            try:
                cursor_entity.execute("""
                    SELECT property_value FROM entity_property
                    WHERE entity_id = %s AND property_id = %s
                """, (correct_id, property_id))
                correct_val = cursor_entity.fetchone()  # Consuming the result

                if correct_val is not None:
                    correct_val = correct_val[0]
                else:
                    correct_val = None

                cursor_entity.execute("""
                    SELECT property_value FROM entity_property
                    WHERE entity_id = %s AND property_id = %s
                """, (incorrect_id, property_id))
                incorrect_val = cursor_entity.fetchone()  # Consuming the result

                if incorrect_val is not None:
                    incorrect_val = incorrect_val[0]
                else:
                    incorrect_val = None

                if (correct_val is None ) and incorrect_val is not None:
                    cursor_entity.execute("""
                        UPDATE entity_property
                        SET property_value = %s
                        WHERE entity_id = %s AND property_id = %s
                    """, (incorrect_val, correct_id,property_id))

                    log(f"✅ updated property `{property_id}` from {incorrect_id} → {correct_id}")

                cursor_entity.execute("""
                    DELETE FROM entity_property
                    WHERE entity_id = %s AND property_id = %s
                """, (incorrect_id, property_id))

                conn_entity.commit()   
                # ----------------------------------------------------------------------------- # 
                if entity_type == 1:
                    update_entity_mapping_type1(conn_entity, correct_id, incorrect_id)
                elif entity_type == 2:
                    update_entity_mapping_type2(conn_entity, correct_id, incorrect_id)
                # ----------------------------------------------------------------------------- # 
                update_entity_role_links(conn_entity, correct_id, incorrect_id)
                #------------------------------------------------------------------------------ #

                if entity_type == 1:
                    cursor_entity.execute("DELETE FROM entity WHERE entity_id = %s", (incorrect_id,))
                elif entity_type == 2:
                    cursor_entity.execute("DELETE FROM people WHERE entity_id = %s", (incorrect_id,))
                    cursor_entity.execute("DELETE FROM entity WHERE entity_id = %s", (incorrect_id,))

                conn_entity.commit()  

            except Exception as e:
                log(f"❌ Failed moving property `{property_id}` for {incorrect_id}: {e}")

        
    conn_entity.commit()  
    conn_other.commit() 

    cursor_entity.close()
    cursor_dms.close()
    conn_entity.close()
    conn_other.close()

    print(f"cnt is {cnt}")
    log("🎯 All updates completed.")


# Usage example:
update_properties_and_phone("comcomxemails.csv")



# def update_leads_transaction_ids(master_id, other_id, cursor, type, json_path):

#     with open(json_path) as f:
#         id_map = json.load(f)

#     # Fetch master and incorrect entity IDs from JSON
#     master_people_entitiy_id = id_map.get(str(master_id))
#     if not master_people_entitiy_id:
#         print(f"[SKIP] master_id {master_id} not found in JSON file.")
#         return
    
#     incorrect_entity_id = id_map.get(str(other_id))
#     if not incorrect_entity_id:
#         print(f"[SKIP] other_id {other_id} not found in JSON file.")
#         return 

#     updateFields = [
#         "deal_fields", "deal_fields_values", "deal_sum", "leads_connections",
#         "leads_notes", "leads_sss", "leads_tags", "leads_tickets",
#         "leads_transactions_contacts", "lock_notes"
#     ]
#     try:
#         if type == 1:
#             cursor.execute("SELECT leads_transactions_id FROM global_organisations WHERE id = %s", (master_people_entitiy_id,))
#         elif type == 2:
#             cursor.execute("SELECT leads_transactions_id FROM global_people WHERE id = %s", (master_people_entitiy_id,))
#         else:
#             log(f"❌ Invalid type: {type}. Must be 1 or 2.")
#             return

#         result = cursor.fetchone()
#         if not result:
#             log(f"⚠️ ID {master_people_entitiy_id} not found in type {type}")
#             return

#         master_leads_transaction_id = result[0]
#         if not master_leads_transaction_id or master_leads_transaction_id == 0:
#             log(f"⚠️ ID {master_people_entitiy_id} has no valid leads_transactions_id")
#             return

#         cursor.execute("SELECT id FROM leads_transactions WHERE id = %s", (master_leads_transaction_id,))
#         if not cursor.fetchone():
#             log(f"⚠️ leads_transactions_id {master_leads_transaction_id} does not exist in leads_transactions")
#             return

#         other_ids_str = str(incorrect_entity_id)

#         for table in updateFields:
#             if master_leads_transaction_id != 0:
#                 query = f"""
#                     UPDATE {table}
#                     SET leads_transactions_id = %s
#                     WHERE leads_transactions_id IN ({other_ids_str})
#                 """
#                 cursor.execute(query, (master_leads_transaction_id,))
#                 log(f"✅ Updated leads_transactions_id in `{table}`")

#                 cursor.execute(f"""
#                     UPDATE key_library
#                     SET lead_transcation_id = %s
#                     WHERE lead_transcation_id IN ({other_ids_str})
#                 """, (master_leads_transaction_id,))
#                 log("✅ Updated key_library")

#                 cursor.execute(f"""
#                     SELECT SUM(investment_hours) FROM investment_hours
#                     WHERE leads_transactions_id IN ({other_ids_str}, {master_leads_transaction_id})
#                 """)
#                 total_hours = cursor.fetchone()[0] or 0

#                 cursor.execute("""
#                     UPDATE investment_hours
#                     SET investment_hours = %s
#                     WHERE leads_transactions_id = %s
#                 """, (total_hours, master_leads_transaction_id))
#                 log("✅ Updated investment_hours total")

#                 cursor.execute(f"""
#                     DELETE FROM investment_hours
#                     WHERE leads_transactions_id IN ({other_ids_str})
#                 """)
#                 log("🗑️ Deleted duplicate investment_hours records")
#             else:
#                 log(f"master leads transaction id is 0 , for {master_people_entitiy_id}")

#         # ✅ Commit changes
#         log("💾 Commit successful")

#     except Exception as e:
#         log(f"❌ Error in update_leads_transaction_ids: {e}")