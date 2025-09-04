import argparse
import dataclasses
import json
import re
import shlex
import ssl
import subprocess
import typing
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

class CSCfgClient:
    def __init__(self):
        self.stamps = self.discover_env_stamps()
        self.sw_to_cs_port = {stamp: self.get_switch_system_config(stamp) for stamp in self.stamps}
        self.cache = {}
        self.cache_time = {}
        self.cache_ttl = 300 

    def discover_env_stamps(self) -> typing.List[str]:
        cmd = "cscfg device show -f type=SW role=LF -ojson | jq -r '.[].properties.location.stamp' | sort | uniq"
        output = subprocess.check_output(cmd, shell=True, text=True)
        print("Discovered environment stamps:\n", output.strip())
        
        return [line.strip() for line in output.splitlines() if line.strip() and line.strip() != "null"]

    def get_systems(self, stamp: str) -> typing.Dict[str, typing.Any]:
        if stamp not in self.stamps:
            return {"error": f"Stamp {stamp} not found in discovered environment stamps."}

        cmd = f"cscfg device show -f type=SY location.stamp={stamp} -ojson"
        systems = subprocess.check_output(shlex.split(cmd), text=True)
        name_system_map = {e["name"]: e for e in json.loads(systems)}
        # print("Systems:", name_system_map)
        return name_system_map

    def switch_link_lldp(self, stamp: str) -> typing.List[typing.Dict[str, typing.Any]]:
        if stamp not in self.stamps:
            return [{"error": f"Stamp {stamp} not found in discovered environment stamps."}]
        cmd = f"cscfg switch link lldp -f 'name=~net0..-lf' 'location.stamp={stamp}' -ojson"
        output = subprocess.check_output(shlex.split(cmd), text=True)
        # print("Switch link LLDP discovered:", json.loads(output))
        return json.loads(output)

    def switch_interface_status(self, stamp: str) -> typing.List[typing.Dict[str, typing.Any]]:
        if stamp not in self.stamps:
            return [{"error": f"Stamp {stamp} not found in discovered environment stamps."}]
        cmd = f"cscfg switch interface status -f 'name=~net0..-lf' 'location.stamp={stamp}' -ojson"
        output = subprocess.check_output(shlex.split(cmd), text=True)
        # print("Switch interface status discovered:", json.loads(output))
        return json.loads(output)

    def get_system_status(self, stamp: str) -> typing.Dict[str, typing.Any]:
        if stamp not in self.stamps:
            return {"error": f"Stamp {stamp} not found in discovered environment stamps."}
        cmd = f"cscfg system status -f location.stamp={stamp} -ojson"
        output = subprocess.check_output(shlex.split(cmd), text=True)
        out_status = output[0:output.rindex("]") + 1]
        system_status = {e["name"]: e for e in json.loads(out_status)}
        # print("System status discovered:", system_status)
        return system_status

    def get_switch_system_config(self, stamp:str) -> typing.List[typing.Tuple[str, str]]:
        if stamp not in self.stamps:
            return [{"error": f"Stamp {stamp} not found in discovered environment stamps."}]
        
        cmd_switches = f"cscfg device show -f name=~net type=SW role=LF location.stamp={stamp} -ojson | jq -r '.[].name'"
        switches = subprocess.check_output(cmd_switches, shell=True, text=True).strip().splitlines()

        sw_to_cs_port = set()
        for switch in switches:
            # discover link of switches
            cmd_switch_link_show = f"cscfg switch link show -f 'name={switch}' -ojson | jq -r '.[] | [.dst_name, .dst_if] | @csv'"
            link_dsts = [
                [item.strip('"') for item in line.split(",")]
                for line in subprocess.check_output(cmd_switch_link_show, shell=True, text=True).strip().splitlines()
            ]
            
            for dst in link_dsts:
                if dst[1].startswith("Port") and (dst[0].endswith("sy01") or dst[0].endswith("sy02")):
                    sw_to_cs_port.add((switch, dst[1]))
            
        return list(sw_to_cs_port)

    def get_system_link_status(self, stamp:str, forced:bool) -> typing.List[typing.List[str]]:
        if stamp not in self.stamps:
            return [["error", f"Stamp {stamp} not found in discovered environment stamps."]]

        if stamp in self.cache and time.time() - self.cache_time.get(stamp, 0) < self.cache_ttl and not forced:
            return self.cache[stamp]

        # If cache is expired or not available, fetch fresh data
        with ThreadPoolExecutor() as executor:
            systems_future = executor.submit(self.get_systems, stamp)
            link_lldp_future = executor.submit(self.switch_link_lldp, stamp)
            switch_interface_future = executor.submit(self.switch_interface_status, stamp)
            system_status_future = executor.submit(self.get_system_status, stamp)

            systems = systems_future.result()
            link_lldp = link_lldp_future.result()
            switch_interface = switch_interface_future.result()
            system_status = system_status_future.result()

        # can have mapping that is e[status] e[comment]
        link_map = {(e["src"], e["src_if"], e["dst"], e["dst_if"]): e for e in link_lldp if e["dst"].endswith("sy01") or e["dst"].endswith("sy02")}

        # csv header 
        csv = []
        header = ["rack_id", "rack", "position", "system_name", "alias", "system_state", "switch_port", *sorted([f"{k.strip()}/{v.strip()}" for k, v in self.sw_to_cs_port[stamp]])]
        csv.append(header)


        for alias, system in systems.items():
            rack_id = system["properties"]["location"]["position"]
            rack = system["properties"]["location"]["rack"]
            position = "T" if system["name"].endswith("sy02") else "B"
            system_name = system["properties"]["management_info"]["name"]
            system_state = system_status.get(system["name"], {}).get("status", "UNKNOWN").upper()

            system_links = [(e["src"], e["src_if"], e["dst"], e["dst_if"]) for e in link_lldp if e["dst"] == alias]
            print(f"System links for {alias}: {system_links}")

            # Check if all src_if values are the same
            src_if_set = set(link[1] for link in system_links)
            for src_if in src_if_set:
                # each system - src_if pair gets a row
                switch_port = src_if
                # Create a row with the static columns
                row = [rack_id, rack, position, system_name, alias, system_state, switch_port]

                status = []

                # if we can get information from link map for each cell, display status/comment, otherwise mark as empty
                for sw, port in sorted(self.sw_to_cs_port[stamp], key=lambda x: (x[0].strip(), x[1].strip())):
                    
                    rec = link_map.get((sw, src_if, alias, port))
                    print(f"Checking link for {sw} {src_if} {alias} {port}:")

                    if rec:
                        stat = rec.get("state", "unknown")
                        if stat == "match":
                            status.append("ok")
                        elif stat == "mismatch":
                            status.append(f"mismatch/{rec['comment']}")
                        elif stat == "unrecognized":
                            status.append(f"unrecognized/{rec['comment']}")
                        elif stat == "missing":
                            status.append(f"down/{rec['comment']}")
                        else:
                            status.append(f"not_found/{rec['comment']}")
                    else:
                        # if no link found, mark entry as empty
                        status.append("-")

                row.extend(status)
                csv.append(row)

            # sort by switch port
            csv[1:] = sorted(csv[1:], key=lambda x: tuple(map(int, x[6].lower().replace("ethernet", "").split("/"))))
        
        self.cache[stamp] = csv
        self.cache_time[stamp] = time.time()
        return csv


        

cli = CSCfgClient()



def run_check(stamp: str, forced: bool) -> typing.List[typing.List[str]]:
    if stamp not in cli.stamps:
        return [["error", f"Stamp {stamp} not found in discovered environment stamps."]]
    return cli.get_system_link_status(stamp, forced)


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        if parsed_url.path == '/api/data':
            # default to the first stamp if none is provided
            stamp = query_params.get("stamp", cli.stamps[0])[0]
            forced = query_params.get("forced", ["false"])[0].lower() == "true"
            output = run_check(stamp, forced)
            cache_info = {
                "stamp": stamp,
                "has_cache": stamp in cli.cache,
                "cache_time": cli.cache_time.get(stamp, 0),
                "cache_age_seconds": time.time() - cli.cache_time.get(stamp, 0) if stamp in cli.cache_time else 0,
                "cache_ttl": cli.cache_ttl
            }
            response_data = {
            "csv": "\n".join([",".join(row) for row in output]),
            "cache_info": cache_info
            }
            response = json.dumps(response_data)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(response.encode())
        elif parsed_url.path.startswith("/status/"):
            stamp = parsed_url.path[len("/status/"):]

            # Return a landing page or HTML with JS that fetches /data asynchronously
            response = f"""
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <title>{stamp} Links - Loading...</title>
                    <style>
                        table {{
                            border-collapse: collapse;
                            width: 100%;
                            font-family: Arial, sans-serif;
                            font-size: 12px;
                        }}
                        th, td {{
                            border: 1px solid #ddd;
                            padding: 2px;
                            text-align: left;
                            white-space: nowrap;
                            overflow: hidden;
                            text-overflow: ellipsis;
                        }}
                        th {{
                            background-color: #f2f2f2;
                            position: sticky;
                            top: 0;
                        }}
                        tr:nth-child(even) {{
                            background-color: #f9f9f9;
                        }}
                        .status-ok {{
                            background-color: #d4edda;
                            color: #155724;
                        }}
                        .status-mismatch {{
                            background-color: #fff3cd;
                            color: #856404;
                        }}
                        .status-error {{
                            background-color: #f8d7da;
                            color: #721c24;
                        }}
                        #tableContainer {{
                            margin-top: 20px;
                            overflow-x: auto;
                        }}
                        .button-container {{
                            margin: 20px 0;
                            display: flex;
                            gap: 10px;
                        }}
                        button {{
                            padding: 8px 16px;
                            background-color: #4CAF50;
                            color: white;
                            border: none;
                            border-radius: 4px;
                            cursor: pointer;
                        }}
                        button:hover {{
                            background-color: #45a049;
                        }}
                        .copy-feedback {{
                            margin-left: 10px;
                            color: #4CAF50;
                            display: none;
                        }}
                        .cache-info {{
                            background-color: #e8f4f8;
                            border: 1px solid #bee5eb;
                            border-radius: 4px;
                            padding: 10px;
                            margin: 10px 0;
                            font-size: 12px;
                            color: #0c5460;
                        }}
                        .cache-fresh {{
                            background-color: #d4edda;
                            border-color: #c3e6cb;
                            color: #155724;
                        }}
                        .cache-stale {{
                            background-color: #fff3cd;
                            border-color: #ffeaa7;
                            color: #856404;
                        }}
                    </style>
                </head>
                <body>
                    <div id="loading">Loading, it takes about 30 seconds to scan the systems, please wait...</div>
                    <div id="cacheInfo" class="cache-info" style="display: none;">
                        <strong>Cache Information:</strong>
                        <span id="cacheDetails"></span>
                    </div>
                    <div class="button-container" style="display: none;" id="buttonContainer">
                        <button id="refreshData"> Refresh Data</button>
                        <button id="copySystemNames">Copy System Names with All OK</button>
                        <span id="systemNamesFeedback" class="copy-feedback">Copied!</span>
                        <button id="copyCSV">Copy CSV</button>
                        <span id="csvFeedback" class="copy-feedback">Copied!</span>

                    </div>
                    <div id="tableContainer" style="display: none;"></div>

                    <script>
                        function displayCacheInfo(cacheData) {{
                            const cacheInfoDiv = document.getElementById('cacheInfo');
                            const cacheDetailsSpan = document.getElementById('cacheDetails');
                            
                            if (cacheData.has_cache) {{
                                const cacheDate = new Date(cacheData.cache_time * 1000);
                                const ageMinutes = Math.floor(cacheData.cache_age_seconds / 60);
                                const ageSeconds = Math.floor(cacheData.cache_age_seconds % 60);
                                const ttlMinutes = Math.floor(cacheData.cache_ttl / 60);
                                
                                const isStale = cacheData.cache_age_seconds > cacheData.cache_ttl;
                                
                                cacheDetailsSpan.innerHTML = `
                                    Data cached at ${{cacheDate.toLocaleString()}} 
                                    (Age: ${{ageMinutes}}m ${{ageSeconds}}s, TTL: ${{ttlMinutes}}m)
                                    ${{isStale ? '<strong style="color: #856404;"> - STALE</strong>' : '<strong style="color: #155724;"> - FRESH</strong>'}}
                                `;
                                
                                // Apply appropriate styling
                                cacheInfoDiv.className = isStale ? 'cache-info cache-stale' : 'cache-info cache-fresh';
                            }} else {{
                                cacheDetailsSpan.innerHTML = 'No cached data available';
                                cacheInfoDiv.className = 'cache-info';
                            }}
                            
                            cacheInfoDiv.style.display = 'block';
                        }}

                        let originalCSV = '';
                        let parsedData = null;
                        let tableReference = null;

                        function parseCSV(csvText) {{
                            const lines = csvText.trim().split('\\n');
                            const headers = lines[0].split(',');
                            const result = [];

                            for (let i = 1; i < lines.length; i++) {{
                                const currentLine = lines[i].split(',');
                                const row = {{}};

                                for (let j = 0; j < headers.length; j++) {{
                                    row[headers[j]] = currentLine[j];
                                }}

                                result.push(row);
                            }}

                            return {{ headers, data: result }};
                        }}

                        function createTable(csvData) {{
                            originalCSV = csvData;
                            parsedData = parseCSV(csvData);
                            const {{ headers, data }} = parsedData;
                            const table = document.createElement('table');
                            tableReference = table;

                            // Create table header
                            const thead = document.createElement('thead');
                            const headerRow = document.createElement('tr');

                            headers.forEach(header => {{
                                const th = document.createElement('th');
                                th.textContent = header;
                                th.title = header; // Add tooltip for full text
                                headerRow.appendChild(th);
                            }});

                            thead.appendChild(headerRow);
                            table.appendChild(thead);

                            // Create table body
                            const tbody = document.createElement('tbody');

                            data.forEach(row => {{
                                const tr = document.createElement('tr');

                                headers.forEach(header => {{
                                    const td = document.createElement('td');
                                    const cellValue = row[header];
                                    td.textContent = cellValue;
                                    td.title = cellValue; // Add tooltip for full text

                                    // Apply color coding based on cell content
                                    if (cellValue === 'ok') {{
                                        td.className = 'status-ok';
                                    }} else if (cellValue.includes('mismatch/')) {{
                                        td.className = 'status-mismatch';
                                    }} else if (cellValue.includes('missing/') || cellValue.includes('down/')) {{
                                        td.className = 'status-error';
                                    }}

                                    tr.appendChild(td);
                                }});

                                tbody.appendChild(tr);
                            }});

                            table.appendChild(tbody);
                            return table;
                        }}
                        
                        function copyAsRichText(htmlContent) {{
                            // Create a temporary div for holding the content
                            const tempDiv = document.createElement('div');
                            tempDiv.innerHTML = htmlContent;
                            tempDiv.style.position = 'fixed';
                            tempDiv.style.left = '-9999px';
                            tempDiv.setAttribute('contenteditable', 'true');
                            
                            // Add to the body
                            document.body.appendChild(tempDiv);
                            
                            // Select the content
                            const range = document.createRange();
                            range.selectNodeContents(tempDiv);
                            
                            const selection = window.getSelection();
                            selection.removeAllRanges();
                            selection.addRange(range);
                            
                            // Execute copy command
                            const successful = document.execCommand('copy');
                            
                            // Clean up
                            document.body.removeChild(tempDiv);
                            selection.removeAllRanges();
                            
                            return successful;
                        }}

                        function setupCopyButtons() {{
                            // Add refresh button functionality
                            document.getElementById('refreshData').addEventListener('click', function() {{
                                // Show loading state
                                document.getElementById('tableContainer').style.display = 'none';
                                document.getElementById('buttonContainer').style.display = 'none';
                                document.getElementById('loading').style.display = 'block';
                                document.getElementById('loading').textContent = 'Refreshing data, please wait...';
                                
                                // Clear existing table
                                const tableContainer = document.getElementById('tableContainer');
                                tableContainer.innerHTML = '';
                                
                                // Fetch fresh data with forced=true
                                fetch('/api/data?stamp={stamp}&forced=true')
                                    .then(res => res.json())
                                    .then(data => {{
                                        document.getElementById('loading').style.display = 'none';
                                        document.getElementById('buttonContainer').style.display = 'flex';
                                        tableContainer.style.display = 'block';

                                        const table = createTable(data.csv);
                                        tableContainer.appendChild(table);
                                        setupCopyButtons();
                                        // Display cache info
                                        displayCacheInfo(data.cache_info);
                                        
                                        // Show refresh feedback
                                        const feedback = document.getElementById('refreshFeedback');
                                        feedback.style.display = 'inline';
                                        setTimeout(() => {{
                                            feedback.style.display = 'none';
                                        }}, 2000);
                                        
                                        document.title = '{stamp} Links';
                                    }})
                                    .catch(err => {{
                                        document.getElementById('loading').textContent = 'Error refreshing data.';
                                        console.error(err);
                                    }});
                            }});

                            // Copy system names with all OK
                            document.getElementById('copySystemNames').addEventListener('click', function() {{
                                const allOkSystems = [];
                                
                                parsedData.data.forEach(row => {{
                                    // Count how many "ok" values exist in this row
                                    let okCount = 0;
                                    for (const key in row) {{
                                        if (row[key] === 'ok') {{
                                            okCount++;
                                        }}
                                    }}
                                    
                                    // If there are 12 "ok" values, add the system name
                                    if (okCount === 12) {{
                                        allOkSystems.push(row.alias);
                                    }}
                                }});
                                
                                copyToClipboard(allOkSystems.join('\\n'), 'systemNamesFeedback');
                            }});
                            
                            // Copy CSV
                            document.getElementById('copyCSV').addEventListener('click', function() {{
                                copyToClipboard(originalCSV, 'csvFeedback');
                            }});
                            
                            // Add new button for rich text
                            const richTextButton = document.createElement('button');
                            richTextButton.id = 'copyRichText';
                            richTextButton.textContent = 'Copy as Rich Text for Email';
                            document.getElementById('buttonContainer').appendChild(richTextButton);
                            
                            const richTextFeedback = document.createElement('span');
                            richTextFeedback.id = 'richTextFeedback';
                            richTextFeedback.className = 'copy-feedback';
                            richTextFeedback.textContent = 'Copied!';
                            document.getElementById('buttonContainer').appendChild(richTextFeedback);
                            
                            // Copy as Rich Text event handler
                            document.getElementById('copyRichText').addEventListener('click', function() {{
                                // Create a copy of the table for email-friendly HTML
                                const tableClone = tableReference.cloneNode(true);
                                
                                // Apply inline styles for email compatibility (same as your existing code)
                                const rows = tableClone.querySelectorAll('tr');
                                rows.forEach((row, rowIndex) => {{
                                    const cells = row.querySelectorAll('th, td');
                                    cells.forEach(cell => {{
                                        // Base styles for all cells
                                        cell.style.border = '1px solid #ddd';
                                        cell.style.padding = '4px';
                                        cell.style.textAlign = 'left';
                                        cell.style.whiteSpace = 'nowrap';
                                        
                                        // Header cells
                                        if (cell.tagName === 'TH') {{
                                            cell.style.backgroundColor = '#f2f2f2';
                                            cell.style.fontWeight = 'bold';
                                        }}
                                        // Even rows
                                        else if (rowIndex % 2 === 1) {{
                                            cell.style.backgroundColor = '#f9f9f9';
                                        }}
                                        
                                        // Status colors
                                        if (cell.classList.contains('status-ok')) {{
                                            cell.style.backgroundColor = '#d4edda';
                                            cell.style.color = '#155724';
                                        }} else if (cell.classList.contains('status-mismatch')) {{
                                            cell.style.backgroundColor = '#fff3cd';
                                            cell.style.color = '#856404';
                                        }} else if (cell.classList.contains('status-error')) {{
                                            cell.style.backgroundColor = '#f8d7da';
                                            cell.style.color = '#721c24';
                                        }}
                                    }});
                                }});
                                
                                // Apply table styles
                                tableClone.style.borderCollapse = 'collapse';
                                tableClone.style.fontFamily = 'Arial, sans-serif';
                                tableClone.style.fontSize = '12px';
                                tableClone.style.width = 'auto'; // Changed from 100% for better email compatibility
                                
                                // Get HTML string and copy as rich text
                                const success = copyAsRichText(tableClone.outerHTML);
                                
                                if (success) {{
                                    // Show feedback
                                    const feedback = document.getElementById('richTextFeedback');
                                    feedback.style.display = 'inline';
                                    setTimeout(() => {{
                                        feedback.style.display = 'none';
                                    }}, 2000);
                                }}
                            }});
                        }}

                        function copyToClipboard(text, feedbackId) {{
                            navigator.clipboard.writeText(text).then(function() {{
                                // Show feedback
                                const feedback = document.getElementById(feedbackId);
                                feedback.style.display = 'inline';
                                setTimeout(() => {{
                                    feedback.style.display = 'none';
                                }}, 2000);
                            }}, function(err) {{
                                console.error('Could not copy text: ', err);
                            }});
                        }}

                        // Fetch data and create the table
                        fetch('/api/data?stamp={stamp}')
                            .then(res => res.json())
                            .then(data => {{
                                document.getElementById('loading').style.display = 'none';
                                document.getElementById('buttonContainer').style.display = 'flex';
                                const tableContainer = document.getElementById('tableContainer');
                                tableContainer.style.display = 'block';

                                // Create and append the table
                                const table = createTable(data.csv);
                                tableContainer.appendChild(table);

                                // Setup copy buttons
                                setupCopyButtons();
                                // Display cache info
                                displayCacheInfo(data.cache_info);

                                document.title = "{stamp} Links";
                            }})
                            .catch(err => {{
                                document.getElementById('loading').textContent = 'Error loading data.';
                                console.error(err);
                            }});
                    </script>
                </body>
                </html>
            """
 
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(response.encode())
        elif parsed_url.path == "/":
            # Create a landing page with links to all stamps
            stamp_links = []
            for stamp in sorted(cli.stamps):
                stamp_links.append(f'<li><a href="/status/{stamp}" class="stamp-link">{stamp}</a></li>')
            
            response = f"""
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <title>System Dashboard - Landing Page</title>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            max-width: 800px;
                            margin: 50px auto;
                            padding: 20px;
                            background-color: #f5f5f5;
                        }}
                        .container {{
                            background-color: white;
                            padding: 30px;
                            border-radius: 8px;
                            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                        }}
                        h1 {{
                            color: #333;
                            text-align: center;
                            margin-bottom: 30px;
                        }}
                        h2 {{
                            color: #555;
                            border-bottom: 2px solid #4CAF50;
                            padding-bottom: 10px;
                            margin-top: 30px;
                        }}
                        .stamps-grid {{
                            display: grid;
                            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                            gap: 15px;
                            margin: 20px 0;
                        }}
                        .stamp-link {{
                            display: block;
                            padding: 15px;
                            background-color: #4CAF50;
                            color: white;
                            text-decoration: none;
                            border-radius: 6px;
                            text-align: center;
                            font-weight: bold;
                            transition: background-color 0.3s;
                        }}
                        .stamp-link:hover {{
                            background-color: #45a049;
                        }}
                        .api-link {{
                            display: inline-block;
                            padding: 12px 20px;
                            background-color: #2196F3;
                            color: white;
                            text-decoration: none;
                            border-radius: 6px;
                            text-align: center;
                            margin: 10px 5px;
                            font-weight: bold;
                        }}
                        .api-link:hover {{
                            background-color: #1976D2;
                        }}
                        .info-box {{
                            background-color: #e3f2fd;
                            border-left: 4px solid #2196F3;
                            padding: 15px;
                            margin: 20px 0;
                        }}
                        .stats {{
                            display: flex;
                            justify-content: space-around;
                            margin: 20px 0;
                            text-align: center;
                        }}
                        .stat {{
                            background-color: #f8f9fa;
                            padding: 15px;
                            border-radius: 6px;
                            border: 1px solid #dee2e6;
                        }}
                        .stat-number {{
                            font-size: 24px;
                            font-weight: bold;
                            color: #4CAF50;
                        }}
                        .stat-label {{
                            color: #666;
                            font-size: 14px;
                        }}
                        ul {{
                            list-style: none;
                            padding: 0;
                        }}
                        li {{
                            margin: 10px 0;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>🖥️ System Dashboard</h1>
                        
                        <div class="stats">
                            <div class="stat">
                                <div class="stat-number">{len(cli.stamps)}</div>
                                <div class="stat-label">Available Stamps</div>
                            </div>
                        </div>

                        <div class="info-box">
                            <strong>ℹ️ About:</strong> This dashboard provides system link status monitoring for different environment stamps. 
                            Click on any stamp below to view detailed system information and connectivity status.
                        </div>

                        <h2>📊 Dashboard</h2>
                        <div class="stamps-grid">
                            {''.join([f'<a href="/status/{stamp}" class="stamp-link">{stamp}</a>' for stamp in sorted(cli.stamps)])}
                        </div>

                        <h2> 🗒️ CSV</h2>
                        <div class="stamps-grid">
                            {''.join([f'<a href="/api/data?stamp={stamp}" class="api-link">{stamp}</a>' for stamp in sorted(cli.stamps)])}
                        </div>
                        <p>Direct API access for programmatic usage:</p>
                        
                        <div class="info-box">
                            <strong>API Usage:</strong><br>
                            • <code>/api/data?stamp=&lt;stamp&gt;</code> - Returns CSV data for specific stamp<br>
                            • <code>/status/&lt;stamp&gt;</code> - Interactive dashboard for specific stamp
                        </div>


                    </div>
                </body>
                </html>
            """
            
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(response.encode())
        else:
            # Handle any other paths, e.g., 404
            self.send_error(404, "Not Found")


def run_server(port):
    server_address = ("", port)
    httpd = HTTPServer(server_address, RequestHandler)

    # Enable SSL
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile="server.crt", keyfile="server.key")
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)

    print(f"Starting secure server on port {port}...")
    httpd.serve_forever()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=int, help="Run as a server on the given port")
    args = parser.parse_args()

    if args.server:
        run_server(args.server)
    else:
        for row in run_check():
            print(",".join(row))


if __name__ == "__main__":
    main()