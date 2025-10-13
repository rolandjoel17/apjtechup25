from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from pyspark.sql.functions import col

catalog_name = "apjtechup.setup"
table_name = "techupusers"
email_column = "user_email"
sql_warehouse_entitlement = "sql_warehouse_can_manage_all_tasks"

w = WorkspaceClient()

df = spark.sql(f"select * from {catalog_name}.{table_name} where user_email like '%wasim%'")
emails_to_add = [row[email_column] for row in df.select(col(email_column)).collect()]
print(f"Found {len(emails_to_add)} user emails in the table.")

for user_email in emails_to_add:
    if not user_email or user_email.strip() == "":
        print("Skipping empty user_email.")
        continue

    user_display_name = user_email.split('@')[0].replace('.', ' ').title()

    try:
        try:
            user = w.users.get(user_email)
            print(f"User '{user_email}' already exists. Updating entitlements...")
        except Exception:
            print(f"Creating user '{user_email}'...")
            w.users.create(
                user_name=user_email,
                display_name=user_display_name
            )
            # Fetch the user again to ensure it is fully provisioned
            # user = w.users.get(user_email)
            # print(f"User '{user.display_name}' created successfully with ID: {user.id}")

        # current_entitlements = [e.value for e in (user.entitlements or [])]
        # if sql_warehouse_entitlement not in current_entitlements:
        #     w.users.update(
        #         id=user.id,
        #         entitlements=[iam.ComplexValue(value=sql_warehouse_entitlement)]
        #     )
        #     print(f"Entitlement '{sql_warehouse_entitlement}' successfully added to user '{user.display_name}'.")
        # else:
        #     print(f"User '{user.display_name}' already has the '{sql_warehouse_entitlement}' entitlement.")

    except Exception as e:
        print(f"Failed to create or update user '{user_email}': {e}")
        break

print("\nScript completed successfully.")