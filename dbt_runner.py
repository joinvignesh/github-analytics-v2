"""
dbt Runner Flask Application
Purpose: Expose HTTP endpoint to trigger dbt build via Cloud Scheduler
"""

import os
import subprocess
import logging
import json
from datetime import datetime
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration from environment variables
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR', '/app/dbt_project')
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR', '/root/.dbt')


def run_dbt_command(command: list, cwd: str = None) -> dict:
    """
    Run a dbt command and capture output.
    
    Args:
        command: List of command parts (e.g., ['dbt', 'build'])
        cwd: Working directory for the command
        
    Returns:
        dict with status, stdout, stderr, return_code
    """
    try:
        logger.info(f"Executing dbt command: {' '.join(command)}")
        
        result = subprocess.run(
            command,
            cwd=cwd or DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minute timeout
        )
        
        return {
            'success': result.returncode == 0,
            'return_code': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
        
    except subprocess.TimeoutExpired:
        logger.error("dbt command timed out after 30 minutes")
        return {
            'success': False,
            'return_code': -1,
            'stdout': '',
            'stderr': 'Command timed out after 30 minutes'
        }
    except Exception as e:
        logger.error(f"Error executing dbt command: {str(e)}")
        return {
            'success': False,
            'return_code': -1,
            'stdout': '',
            'stderr': str(e)
        }


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Cloud Run"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()}), 200


@app.route('/run', methods=['POST'])
def run_dbt():
    """
    Main endpoint to trigger dbt build.
    Accepts POST requests from Cloud Scheduler.
    """
    start_time = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("dbt Build Triggered")
    logger.info(f"Start Time: {start_time.isoformat()}")
    logger.info("=" * 60)
    
    try:
        # Parse request body (optional parameters)
        request_data = request.get_json(silent=True) or {}
        
        # Allow custom dbt commands (default: build)
        dbt_command = request_data.get('command', 'build')
        models = request_data.get('models', None)  # Optional: specific models to run
        full_refresh = request_data.get('full_refresh', False)
        
        # Build dbt command
        cmd = ['dbt', dbt_command]
        
        if models:
            cmd.extend(['--select', models])
            
        if full_refresh and dbt_command in ['run', 'build']:
            cmd.append('--full-refresh')
        
        # Run dbt debug first (verify connection)
        logger.info("Step 1: Running dbt debug to verify connection")
        debug_result = run_dbt_command(['dbt', 'debug'])
        
        if not debug_result['success']:
            logger.error("dbt debug failed - connection issue")
            logger.error(f"Error: {debug_result['stderr']}")
            return jsonify({
                'success': False,
                'error': 'dbt debug failed - connection to Snowflake failed',
                'details': debug_result['stderr']
            }), 500
        
        logger.info("✅ dbt debug passed - connection successful")
        
        # Run dbt deps (install packages)
        logger.info("Step 2: Running dbt deps to install packages")
        deps_result = run_dbt_command(['dbt', 'deps'])
        
        if not deps_result['success']:
            logger.warning("dbt deps had issues (continuing anyway)")
            logger.warning(f"Warning: {deps_result['stderr']}")
        else:
            logger.info("✅ dbt deps completed")
        
        # Run main dbt command
        logger.info(f"Step 3: Running dbt {dbt_command}")
        logger.info(f"Command: {' '.join(cmd)}")
        
        result = run_dbt_command(cmd)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info("dbt Build Completed")
        logger.info(f"End Time: {end_time.isoformat()}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Success: {result['success']}")
        logger.info("=" * 60)
        
        if result['success']:
            # Parse dbt output for summary
            summary = parse_dbt_summary(result['stdout'])
            
            response = {
                'success': True,
                'message': f'dbt {dbt_command} completed successfully',
                'duration_seconds': duration,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'summary': summary,
                'logs': result['stdout'][-2000:]  # Last 2000 chars of output
            }
            
            logger.info(f"Summary: {json.dumps(summary, indent=2)}")
            return jsonify(response), 200
            
        else:
            logger.error(f"dbt {dbt_command} failed")
            logger.error(f"Error output: {result['stderr']}")
            
            return jsonify({
                'success': False,
                'message': f'dbt {dbt_command} failed',
                'duration_seconds': duration,
                'error': result['stderr'],
                'logs': result['stdout'][-2000:]
            }), 500
            
    except Exception as e:
        logger.error(f"Unexpected error in run_dbt: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}'
        }), 500


@app.route('/test', methods=['POST'])
def test_connection():
    """
    Test endpoint to verify dbt can connect to Snowflake.
    Runs dbt debug only.
    """
    logger.info("Testing dbt connection (dbt debug)")
    
    result = run_dbt_command(['dbt', 'debug'])
    
    if result['success']:
        return jsonify({
            'success': True,
            'message': 'dbt debug passed - Snowflake connection successful',
            'output': result['stdout']
        }), 200
    else:
        return jsonify({
            'success': False,
            'message': 'dbt debug failed',
            'error': result['stderr'],
            'output': result['stdout']
        }), 500


def parse_dbt_summary(output: str) -> dict:
    """
    Parse dbt output to extract summary statistics.
    """
    summary = {
        'models_run': 0,
        'tests_run': 0,
        'passed': 0,
        'warnings': 0,
        'errors': 0,
        'skipped': 0
    }
    
    try:
        # Look for summary line pattern
        for line in output.split('\n'):
            if 'PASS=' in line:
                # Example: "Done. PASS=17 WARN=0 ERROR=0 SKIP=0 TOTAL=17"
                parts = line.split()
                for part in parts:
                    if 'PASS=' in part:
                        summary['passed'] = int(part.split('=')[1])
                    elif 'WARN=' in part:
                        summary['warnings'] = int(part.split('=')[1])
                    elif 'ERROR=' in part:
                        summary['errors'] = int(part.split('=')[1])
                    elif 'SKIP=' in part:
                        summary['skipped'] = int(part.split('=')[1])
                    elif 'TOTAL=' in part:
                        summary['total'] = int(part.split('=')[1])
    except Exception as e:
        logger.warning(f"Could not parse dbt summary: {str(e)}")
    
    return summary


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting dbt Runner on port {port}")
    logger.info(f"dbt project directory: {DBT_PROJECT_DIR}")
    logger.info(f"dbt profiles directory: {DBT_PROFILES_DIR}")
    
    app.run(host='0.0.0.0', port=port)
