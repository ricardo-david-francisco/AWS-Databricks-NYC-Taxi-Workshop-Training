# Databricks notebook source
# MAGIC %md
# MAGIC # AI Assistant
# MAGIC
# MAGIC https://www.databricks.com/blog/introducing-databricks-assistant
# MAGIC
# MAGIC Databricks Assistant lets you query data through a conversational interface, making you more productive inside Databricks. Describe your task in English and let the Assistant generate SQL queries, explain complex code, and automatically fix errors. The Assistant leverages Unity Catalog metadata to understand your tables, columns, descriptions, and popular data assets across your company to provide responses that are personalized to you.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Autocomplete code or queries
# MAGIC
# MAGIC You can use the Assistant from inside a notebook cell or query editor to suggest code snippets. Type a comment, and press control + shift + space (Windows) or option + shift + space (Mac) to trigger an autocomplete suggestion.

# COMMAND ----------

# Write code to reverse a string. Then display the string.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Transform code
# MAGIC The Assistant can also transform code from one language or framework to another, so you can always use the best language for the current task. For example, you can take pandas code and convert it into PySpark without needing to rewrite anything.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Fix issues
# MAGIC
# MAGIC Databricks Assistant can identify errors in your code and recommend fixes. When you encounter issues like syntax errors, the Assistant will explain the problem and create a code snippet with a proposed fix.
# MAGIC
# MAGIC Run the code below and press the Diagnos Error button.

# COMMAND ----------

def: check_email(email):
    if(re.fullmatch(regex, email)):
        print("Valid Email") 
    else:
        print("Invalid Email")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain code or query
# MAGIC Databricks Assistant can describe complex pieces of code or queries in clear, concise language. It can help you better understand certain projects you may be unfamiliar with. 
# MAGIC
# MAGIC 1. Press the assistant (chatbox) symbol next to Workspace
# MAGIC 2. Run the cell below
# MAGIC 3. ask the Assistant to explain the code with "Can you explain my check_email() function?"

# COMMAND ----------

import re

regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
 
def check_email(email):
    if(re.fullmatch(regex, email)):
        print("Valid Email") 
    else:
        print("Invalid Email")

check_email("an.email@domain.com.uk")

# COMMAND ----------


