# Stock Total Scores Web Application

A modern web dashboard to view and explore company composite scores based on AI analysis and short interest data.

## Features

- 📊 **Interactive Dashboard**: View all companies with their total composite scores
- 🔍 **Search**: Search companies by ticker or company name
- 📈 **Sorting**: Sort by total score, ticker, company name, or metrics count
- 📱 **Responsive Design**: Works on desktop and mobile devices
- 🎨 **Modern UI**: Beautiful gradient design with smooth animations
- 📋 **Detailed Views**: Click on any company to see detailed metric breakdown
- 📊 **Statistics**: View summary statistics at the top

## Prerequisites

- Python 3.7+
- Flask (install with `pip install -r requirements.txt`)

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Make sure you have generated the database:
```bash
python3 calculate_total_scores.py
```

This will create `total_scores.db` if it doesn't exist.

## Running the Application

Start the web server:
```bash
python3 app.py
```

Then open your browser and navigate to:
```
http://localhost:5000
```

The app will be available on all network interfaces (0.0.0.0) on port 5000.

## API Endpoints

- `GET /` - Main dashboard page
- `GET /api/companies` - Get all companies (supports ?sort=, ?order=, ?search=, ?limit=)
- `GET /api/company/<ticker>` - Get detailed info for a specific company
- `GET /api/stats` - Get summary statistics

## Usage

1. **Search**: Type in the search box to filter companies by ticker or name
2. **Sort**: Use the dropdown to select sorting field and click the button to toggle ascending/descending
3. **View Details**: Click on any row to see a detailed breakdown of all metrics for that company
4. **Close Modal**: Click the X or click outside the modal to close

## Screenshot Features

- Color-coded scores: Green (high), Yellow (medium), Red (low)
- Real-time search and filtering
- Smooth animations and transitions
- Professional gradient header design

