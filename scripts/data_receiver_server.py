#!/usr/bin/env python3
"""
Local Data Receiver Server

A simple Flask server that receives data pushed from browser scripts.
Useful when you want to use browser console or Tampermonkey scripts
to extract data and send it to your local machine.

USAGE:
1. Start this server: python scripts/data_receiver_server.py
2. In browser console or userscript, POST data to http://localhost:5050/ingest
3. Data is saved to data/extracted/ automatically
4. Visit http://localhost:5050/status to see collected data
5. GET http://localhost:5050/export to download as JSON

BROWSER EXAMPLE:
    // In browser console on ProjectSight
    fetch('http://localhost:5050/ingest', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            type: 'rfi',
            id: 'RFI-001',
            title: 'Sample RFI',
            status: 'Open'
        })
    });
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify, Response
from flask_cors import CORS

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for browser requests

# In-memory storage
collected_data = []
session_start = datetime.now()


@app.route('/')
def index():
    """Simple landing page with usage instructions."""
    return """
    <html>
    <head><title>Data Receiver</title></head>
    <body style="font-family: monospace; padding: 20px;">
        <h1>ProjectSight Data Receiver</h1>
        <p>Server is running! Use the endpoints below:</p>

        <h2>Endpoints</h2>
        <ul>
            <li><b>POST /ingest</b> - Send data from browser</li>
            <li><b>GET /status</b> - View collected data summary</li>
            <li><b>GET /export</b> - Download all data as JSON</li>
            <li><b>GET /data</b> - View all data (API)</li>
            <li><b>POST /clear</b> - Clear collected data</li>
        </ul>

        <h2>Browser Console Example</h2>
        <pre style="background: #f0f0f0; padding: 10px;">
// Extract and send data
const items = [...document.querySelectorAll('table tbody tr')].map(row => ({
    cells: [...row.querySelectorAll('td')].map(td => td.textContent.trim())
}));

for (const item of items) {
    await fetch('http://localhost:5050/ingest', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(item)
    });
}
        </pre>

        <h2>Current Status</h2>
        <p>Records collected: <b id="count">loading...</b></p>
        <p><a href="/status">View Details</a> | <a href="/export">Download JSON</a></p>

        <script>
            fetch('/data').then(r => r.json()).then(d => {
                document.getElementById('count').textContent = d.length;
            });
        </script>
    </body>
    </html>
    """


@app.route('/ingest', methods=['POST', 'OPTIONS'])
def ingest():
    """
    Receive data from browser.

    Accepts JSON body with any structure.
    Adds metadata: _received_at, _index
    """
    if request.method == 'OPTIONS':
        # Handle CORS preflight
        return '', 204

    try:
        data = request.get_json(force=True)

        if isinstance(data, list):
            # Batch insert
            for item in data:
                item['_received_at'] = datetime.now().isoformat()
                item['_index'] = len(collected_data)
                collected_data.append(item)
            logger.info(f"Received batch of {len(data)} records (total: {len(collected_data)})")
            return jsonify({
                'status': 'ok',
                'received': len(data),
                'total': len(collected_data)
            })
        else:
            # Single item
            data['_received_at'] = datetime.now().isoformat()
            data['_index'] = len(collected_data)
            collected_data.append(data)
            logger.info(f"Received record #{len(collected_data)}: {str(data)[:100]}...")
            return jsonify({
                'status': 'ok',
                'index': len(collected_data) - 1,
                'total': len(collected_data)
            })

    except Exception as e:
        logger.error(f"Ingest error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 400


@app.route('/status')
def status():
    """Show collection status and summary."""
    summary = {
        'session_start': session_start.isoformat(),
        'records_collected': len(collected_data),
        'unique_types': list(set(d.get('type', 'unknown') for d in collected_data)),
    }

    # Show sample of recent records
    recent = collected_data[-5:] if collected_data else []

    return f"""
    <html>
    <head><title>Data Receiver Status</title></head>
    <body style="font-family: monospace; padding: 20px;">
        <h1>Collection Status</h1>
        <pre>{json.dumps(summary, indent=2)}</pre>

        <h2>Recent Records (last 5)</h2>
        <pre style="background: #f0f0f0; padding: 10px; max-height: 400px; overflow: auto;">
{json.dumps(recent, indent=2, default=str)}
        </pre>

        <p>
            <a href="/">Home</a> |
            <a href="/export">Download All</a> |
            <form method="POST" action="/clear" style="display:inline">
                <button type="submit">Clear Data</button>
            </form>
        </p>
    </body>
    </html>
    """


@app.route('/data')
def get_data():
    """Return all collected data as JSON."""
    return jsonify(collected_data)


@app.route('/export')
def export():
    """Download collected data as JSON file."""
    if not collected_data:
        return jsonify({'error': 'No data to export'}), 404

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"projectsight_export_{timestamp}.json"

    response = Response(
        json.dumps(collected_data, indent=2, default=str),
        mimetype='application/json'
    )
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    return response


@app.route('/save', methods=['POST'])
def save_to_file():
    """Save collected data to file on server."""
    if not collected_data:
        return jsonify({'error': 'No data to save'}), 404

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path('data/extracted')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"projectsight_{timestamp}.json"

    with open(output_path, 'w') as f:
        json.dump(collected_data, f, indent=2, default=str)

    logger.info(f"Saved {len(collected_data)} records to {output_path}")
    return jsonify({
        'status': 'ok',
        'file': str(output_path),
        'records': len(collected_data)
    })


@app.route('/clear', methods=['POST'])
def clear():
    """Clear all collected data."""
    global collected_data
    count = len(collected_data)
    collected_data = []
    logger.info(f"Cleared {count} records")
    return jsonify({'status': 'ok', 'cleared': count})


@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'records': len(collected_data)})


# Bookmarklet generator
@app.route('/bookmarklet')
def bookmarklet():
    """Generate a bookmarklet for quick extraction."""
    return """
    <html>
    <head><title>Extraction Bookmarklet</title></head>
    <body style="font-family: monospace; padding: 20px;">
        <h1>Extraction Bookmarklet</h1>
        <p>Drag this link to your bookmarks bar:</p>

        <p style="font-size: 18px; padding: 10px; background: #e0e0ff;">
            <a href="javascript:(function(){const rows=[...document.querySelectorAll('table tbody tr')].map((row,i)=>({index:i,cells:[...row.querySelectorAll('td')].map(td=>td.textContent.trim())}));if(rows.length===0){alert('No table rows found');return;}fetch('http://localhost:5050/ingest',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(rows)}).then(r=>r.json()).then(d=>alert('Sent '+rows.length+' rows. Total: '+d.total)).catch(e=>alert('Error: '+e));})();">
                📥 Extract Table
            </a>
        </p>

        <h2>What it does:</h2>
        <ol>
            <li>Finds all table rows on the current page</li>
            <li>Extracts cell contents</li>
            <li>Sends to this server</li>
            <li>Shows confirmation alert</li>
        </ol>

        <h2>Custom Extraction</h2>
        <p>For more control, paste this in the browser console:</p>
        <pre style="background: #f0f0f0; padding: 10px;">
// Customize this function for your needs
async function extractAndSend(selector = 'table tbody tr') {
    const rows = [...document.querySelectorAll(selector)].map((row, i) => {
        const data = { index: i };

        // Extract text from cells
        row.querySelectorAll('td, [role="cell"]').forEach((cell, j) => {
            data[`col_${j}`] = cell.textContent.trim();
        });

        return data;
    });

    console.log(`Found ${rows.length} rows`);

    // Send in batches of 50
    for (let i = 0; i < rows.length; i += 50) {
        const batch = rows.slice(i, i + 50);
        const resp = await fetch('http://localhost:5050/ingest', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(batch)
        });
        const result = await resp.json();
        console.log(`Sent batch ${i/50 + 1}, total: ${result.total}`);
    }

    console.log('Done!');
}

// Run it
extractAndSend();
        </pre>

        <p><a href="/">Back to Home</a></p>
    </body>
    </html>
    """


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Local Data Receiver Server')
    parser.add_argument('--port', '-p', type=int, default=5050, help='Port to run on')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    args = parser.parse_args()

    logger.info(f"Starting Data Receiver on http://localhost:{args.port}")
    logger.info("Endpoints:")
    logger.info(f"  POST http://localhost:{args.port}/ingest - Receive data")
    logger.info(f"  GET  http://localhost:{args.port}/status - View status")
    logger.info(f"  GET  http://localhost:{args.port}/export - Download JSON")
    logger.info(f"  GET  http://localhost:{args.port}/bookmarklet - Get bookmarklet")
    logger.info("")
    logger.info("Waiting for data from browser...")

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == '__main__':
    main()
