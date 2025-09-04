from flask import Flask, jsonify, render_template, request
import pymysql
from datetime import datetime
import os
import logging
from functools import wraps


app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_connection():
    connection = pymysql.connect(
        host="mydb.c36c04gwo2cv.eu-central-1.rds.amazonaws.com",  # Keep this hardcoded
        user='dbuser',
        password='dbpassword',
        db='devprojdb',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


def log_request(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        logger.info(
            f"Request: {request.method} {request.path} from {request.remote_addr}")
        return f(*args, **kwargs)
    return decorated_function


@app.route('/health')
@log_request
def health():
    return jsonify({
        "status": "Up & Running",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    })


@app.route('/system-info')
@log_request
def system_info():
    """DevOps monitoring endpoint showing system information"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Test database connectivity
        cursor.execute("SELECT 1 as db_test")
        db_status = "Connected"
        connection.close()
    except Exception as e:
        db_status = f"Error: {str(e)}"

    return jsonify({
        "application": "Flask DevOps Portfolio",
        "status": "running",
        "database_status": db_status,
        "timestamp": datetime.now().isoformat(),
        "environment": os.environ.get('ENVIRONMENT', 'production'),
        "python_version": os.sys.version,
        "endpoints": [
            "/health", "/system-info", "/", "/data",
            "/insert_record", "/create_table", "/stats", "/search"
        ]
    })


@app.route('/create_table')
@log_request
def create_table():
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Create main table
        create_table_query = """
            CREATE TABLE IF NOT EXISTS example_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                department VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """
        cursor.execute(create_table_query)

        # Create a stats table for analytics
        stats_table_query = """
            CREATE TABLE IF NOT EXISTS app_stats (
                id INT AUTO_INCREMENT PRIMARY KEY,
                endpoint VARCHAR(100),
                request_count INT DEFAULT 1,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """
        cursor.execute(stats_table_query)

        connection.commit()
        connection.close()

        logger.info("Database tables created successfully")
        return jsonify({
            "status": "success",
            "message": "Tables created successfully",
            "tables": ["example_table", "app_stats"]
        })
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/insert_record', methods=['POST'])
@log_request
def insert_record():
    try:
        data = request.json
        name = data.get('name')
        email = data.get('email', '')
        department = data.get('department', 'General')

        if not name:
            return jsonify({"status": "error", "message": "Name is required"}), 400

        connection = get_db_connection()
        cursor = connection.cursor()

        insert_query = """
            INSERT INTO example_table (name, email, department) 
            VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (name, email, department))

        # Update stats
        cursor.execute("""
            INSERT INTO app_stats (endpoint, request_count) 
            VALUES ('insert_record', 1) 
            ON DUPLICATE KEY UPDATE 
            request_count = request_count + 1
        """)

        connection.commit()
        record_id = cursor.lastrowid
        connection.close()

        logger.info(f"Record inserted successfully: {name}")
        return jsonify({
            "status": "success",
            "message": "Record inserted successfully",
            "record_id": record_id
        })
    except Exception as e:
        logger.error(f"Error inserting record: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/data')
@log_request
def data():
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Get pagination parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        offset = (page - 1) * per_page

        # Get total count
        cursor.execute('SELECT COUNT(*) as total FROM example_table')
        total = cursor.fetchone()['total']

        # Get paginated data
        cursor.execute("""
            SELECT id, name, email, department, created_at, updated_at 
            FROM example_table 
            ORDER BY created_at DESC 
            LIMIT %s OFFSET %s
        """, (per_page, offset))

        result = cursor.fetchall()
        connection.close()

        # Format timestamps for display
        for record in result:
            if record['created_at']:
                record['created_at'] = record['created_at'].isoformat()
            if record['updated_at']:
                record['updated_at'] = record['updated_at'].isoformat()

        return jsonify({
            "status": "success",
            "data": result,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page
            }
        })
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/search')
@log_request
def search():
    try:
        query = request.args.get('q', '').strip()
        if not query:
            return jsonify({"status": "error", "message": "Search query required"}), 400

        connection = get_db_connection()
        cursor = connection.cursor()

        search_query = """
            SELECT id, name, email, department, created_at 
            FROM example_table 
            WHERE name LIKE %s OR email LIKE %s OR department LIKE %s
            ORDER BY created_at DESC
        """
        search_term = f"%{query}%"
        cursor.execute(search_query, (search_term, search_term, search_term))

        result = cursor.fetchall()
        connection.close()

        # Format timestamps
        for record in result:
            if record['created_at']:
                record['created_at'] = record['created_at'].isoformat()

        return jsonify({
            "status": "success",
            "query": query,
            "results": result,
            "count": len(result)
        })
    except Exception as e:
        logger.error(f"Error in search: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/stats')
@log_request
def stats():
    """Analytics endpoint for portfolio demonstration"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Get total records
        cursor.execute('SELECT COUNT(*) as total FROM example_table')
        total_records = cursor.fetchone()['total']

        # Get records by department
        cursor.execute("""
            SELECT department, COUNT(*) as count 
            FROM example_table 
            GROUP BY department 
            ORDER BY count DESC
        """)
        department_stats = cursor.fetchall()

        # Get recent activity (last 10 records)
        cursor.execute("""
            SELECT name, department, created_at 
            FROM example_table 
            ORDER BY created_at DESC 
            LIMIT 10
        """)
        recent_activity = cursor.fetchall()

        # Format timestamps
        for record in recent_activity:
            if record['created_at']:
                record['created_at'] = record['created_at'].isoformat()

        connection.close()

        return jsonify({
            "status": "success",
            "total_records": total_records,
            "department_breakdown": department_stats,
            "recent_activity": recent_activity,
            "generated_at": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error generating stats: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/delete_record/<int:record_id>', methods=['DELETE'])
@log_request
def delete_record(record_id):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Check if record exists
        cursor.execute(
            "SELECT name FROM example_table WHERE id = %s", (record_id,))
        record = cursor.fetchone()

        if not record:
            return jsonify({"status": "error", "message": "Record not found"}), 404

        # Delete the record
        cursor.execute("DELETE FROM example_table WHERE id = %s", (record_id,))
        connection.commit()
        connection.close()

        logger.info(f"Record deleted: {record['name']} (ID: {record_id})")
        return jsonify({
            "status": "success",
            "message": f"Record '{record['name']}' deleted successfully"
        })
    except Exception as e:
        logger.error(f"Error deleting record: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# Main UI route


@app.route('/')
def index():
    return render_template('index.html')


@app.errorhandler(404)
def not_found(error):
    return jsonify({"status": "error", "message": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({"status": "error", "message": "Internal server error"}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
