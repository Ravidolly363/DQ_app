from flask import Flask, render_template, request, jsonify, session
import mysql.connector
import os
import re
import logging
import json
from datetime import datetime
from dotenv import load_dotenv

# Import Groq - only use the new method
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    print("Groq package not found or outdated. Please install with: pip install groq>=0.4.0")

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.template_folder = 'templates'
app.secret_key = os.environ.get('FLASK_SECRET_KEY', os.urandom(24))

# Database configuration from environment variables
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'user': os.environ.get('DB_USER', 'data_processor'),
    'password': os.environ.get('DB_PASSWORD', 'StrongPassword123'),
    'database': os.environ.get('DB_NAME', 'DataQuality')
}

# Initialize Groq client with API key from environment variable
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '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')

# Only initialize if Groq is available
groq_client = None
if GROQ_AVAILABLE:
    try:
        groq_client = Groq(api_key=GROQ_API_KEY)
        logger.info("Groq client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Groq client: {str(e)}")
        GROQ_AVAILABLE = False

# Main routes
@app.route('/')
def index():
    if 'chat_history' not in session:
        session['chat_history'] = []
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_data():
    user_message = request.json.get('message', '')
    database = request.json.get('database', 'DataQuality')
    logger.info(f"Received user message for database {database}: {user_message}")
    
    if 'chat_history' not in session:
        session['chat_history'] = []
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    session['chat_history'].append({
        'role': 'user',
        'content': user_message,
        'timestamp': timestamp,
        'database': database
    })
    
    if "what is the code" in user_message.lower() or "show me the sql" in user_message.lower():
        return handle_history_request(user_message)
    
    ai_response = get_ai_response(user_message, database)
    logger.info(f"AI response: {ai_response}")
    
    result = execute_ai_commands(ai_response, database)
    logger.info(f"Execution result: {result}")
    
    session['chat_history'].append({
        'role': 'assistant',
        'content': ai_response,
        'result': result,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'database': database
    })
    
    session.modified = True
    
    return jsonify({
        'response': ai_response, 
        'result': result,
        'history': session['chat_history']
    })

def handle_history_request(user_message):
    history = session.get('chat_history', [])
    
    sql_operations = []
    for entry in history:
        if entry['role'] == 'assistant' and 'content' in entry:
            sql_commands = re.findall(r'<SQL>(.*?)</SQL>', entry['content'], re.DOTALL)
            if sql_commands:
                for sql in sql_commands:
                    sql_operations.append({
                        'sql': sql.strip(),
                        'timestamp': entry.get('timestamp', 'Unknown time'),
                        'database': entry.get('database', 'DataQuality')
                    })
    
    if not sql_operations:
        response = "I haven't executed any SQL operations yet in this session."
    else:
        response = "Here are the SQL operations I've executed in this session:\n\n"
        for i, op in enumerate(sql_operations, 1):
            response += f"{i}. At {op['timestamp']} on database {op['database']}:\n<SQL>{op['sql']}</SQL>\n\n"
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    database = history[-1].get('database', 'DataQuality') if history else 'DataQuality'
    
    session['chat_history'].append({
        'role': 'assistant',
        'content': response,
        'timestamp': timestamp,
        'database': database
    })
    session.modified = True
    
    return jsonify({
        'response': response, 
        'result': None,
        'history': session['chat_history']
    })

@app.route('/history', methods=['GET'])
def get_history():
    history = session.get('chat_history', [])
    return jsonify(history)

@app.route('/clear_history', methods=['POST'])
def clear_history():
    session['chat_history'] = []
    session.modified = True
    return jsonify({"status": "success", "message": "History cleared"})

@app.route('/list_databases', methods=['GET'])
def list_databases():
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({
            "status": "success",
            "databases": databases
        })
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/test_db', methods=['POST'])
def test_db():
    database = request.json.get('database', 'DataQuality')
    try:
        db_config = DB_CONFIG.copy()
        db_config['database'] = database
        
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({
            "status": "success", 
            "connection": "OK", 
            "database": database,
            "tables": tables
        })
    except Exception as e:
        logger.error(f"Database connection error for {database}: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})

def get_database_schema(database='DataQuality'):
    try:
        db_config = DB_CONFIG.copy()
        db_config['database'] = database
        
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        if not tables:
            return "No tables found in this database."
        
        schema_info = []
        
        for table in tables:
            cursor.execute(f"DESCRIBE `{table}`")
            columns = cursor.fetchall()
            column_info = [f"{col[0]} ({col[1]})" for col in columns]
            schema_info.append(f"Table '{table}': {', '.join(column_info)}")
        
        cursor.close()
        conn.close()
        
        return "\n".join(schema_info)
    except Exception as e:
        logger.error(f"Error getting schema for database {database}: {str(e)}")
        return f"Unable to retrieve schema for database {database}"

def get_ai_response(user_message, database='DataQuality'):
    # Check if Groq is available
    if not GROQ_AVAILABLE or groq_client is None:
        logger.error("Groq client is not available")
        return "I'm sorry, but the AI service is currently unavailable. Please check that the Groq package is installed correctly (pip install groq>=0.4.0) and that your API key is valid."
    
    history = session.get('chat_history', [])[-15:]
    table_info = get_database_schema(database)
    
    system_prompt = f"""
    You are a friendly, conversational data quality assistant that helps with database operations while also engaging in normal conversation. You can discuss any topic while specializing in data quality concepts.

    DATABASE CONTEXT:
    You are currently working with database: {database}
    The database contains these tables: {table_info}
    
    DATA QUALITY EXPERTISE:
    You understand these data quality dimensions:
    - Completeness: Ensuring data has no missing values
    - Accuracy: Data correctly represents real-world entities
    - Consistency: Data values don't contradict each other
    - Timeliness: Data is up-to-date
    - Validity: Data conforms to defined formats and ranges
    - Uniqueness: No unexpected duplicates exist
    
    WHEN HANDLING SQL AND DATABASE OPERATIONS:
    1. Be EXTREMELY precise with table names - never guess or abbreviate table names
    2. Always verify the exact table name exists before suggesting operations
    3. Format SQL commands within tags like this: <SQL>your SQL here</SQL>
    4. Include the actual SQL command for any data operation
    5. Triple-check any table name that starts with "customer" as these have caused confusion
    
    CONVERSATION MEMORY:
    - Refer to past operations and maintain context throughout the conversation
    - If you've previously executed operations on specific tables, reference them by exact name
    
    DUAL CAPABILITIES:
    - For database requests: Provide accurate SQL and explanations
    - For general questions: Respond conversationally like a helpful assistant
    
    Always prioritize data safety and accuracy in your responses.
    """
    
    messages = [{"role": "system", "content": system_prompt}]
    
    if history:
        sql_summary = f"Previous SQL operations in this conversation (on database {database}):\n"
        operation_count = 0
        
        for msg in history:
            if msg.get('role') == 'assistant' and 'content' in msg:
                sql_commands = re.findall(r'<SQL>(.*?)</SQL>', msg['content'], re.DOTALL)
                for sql in sql_commands:
                    operation_count += 1
                    sql_summary += f"{operation_count}. {sql.strip()}\n"
        
        if operation_count > 0:
            messages.append({"role": "system", "content": sql_summary})
    
    for msg in history:
        if 'content' in msg:
            messages.append({"role": msg['role'], "content": msg['content']})
    
    messages.append({"role": "user", "content": user_message})
    
    try:
        # Use the groq_client that we initialized at the top
        chat_completion = groq_client.chat.completions.create(
            messages=messages,
            model="llama3-70b-8192",
            temperature=0.7,
            top_p=0.9
        )
        return chat_completion.choices[0].message.content
    except AttributeError as e:
        logger.error(f"Groq API AttributeError: {str(e)}")
        return "Error: The Groq package appears to be outdated. Please update it with: pip install --upgrade groq>=0.4.0"
    except Exception as e:
        logger.error(f"Groq API error: {str(e)}")
        return f"Error connecting to AI service: {str(e)}"

def execute_ai_commands(ai_response, database='DataQuality'):
    sql_commands = re.findall(r'<SQL>(.*?)</SQL>', ai_response, re.DOTALL)
    
    if not sql_commands:
        return None
    
    results = []
    for sql in sql_commands:
        sql = sql.strip()
        results.append(execute_sql(sql, database))
    
    return results

def execute_sql(sql, database='DataQuality'):
    try:
        logger.info(f"Executing SQL on database {database}: {sql}")
        
        db_config = DB_CONFIG.copy()
        db_config['database'] = database
        
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute(sql)
        
        if sql.strip().upper().startswith('SELECT'):
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            result = {
                "type": "SELECT",
                "columns": columns,
                "rows": rows,
                "count": len(rows),
                "database": database
            }
        else:
            conn.commit()
            result = {
                "type": sql.split()[0].upper(),
                "rows_affected": cursor.rowcount,
                "status": "success",
                "database": database
            }
            
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"SQL execution error in database {database}: {str(e)}")
        return {
            "type": "ERROR",
            "error": str(e),
            "sql": sql,
            "database": database
        }

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)