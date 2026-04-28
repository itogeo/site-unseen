#!/bin/bash
# Run the full Data Center Sentinel pipeline end to end

set -e

echo "======================================"
echo "  Data Center Sentinel Pipeline"
echo "  Ito Geospatial / Honor the Earth"
echo "======================================"
echo ""

echo "Step 0: Downloading source data..."
python pipeline/00_download_data.py

echo ""
echo "Step 1: Processing tribal boundaries..."
python pipeline/01_process_tribal_lands.py

echo ""
echo "Step 2: Scoring corporate attractiveness..."
python pipeline/02_score_infrastructure.py

echo ""
echo "Step 3: Scoring community vulnerability..."
python pipeline/03_score_vulnerability.py

echo ""
echo "Step 4: Combining scores + classifying risk..."
python pipeline/04_combine_scores.py

echo ""
echo "Step 5: Exporting GeoJSON for Mapbox..."
python pipeline/05_export_geojson.py

echo ""
echo "======================================"
echo "  Pipeline complete!"
echo "  Outputs in: output/"
echo "    - tribal_datacenter_risk.geojson"
echo "    - tribal_datacenter_risk_pts.geojson"
echo "    - known_sites.geojson"
echo "    - stats.json"
echo "======================================"
