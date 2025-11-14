# Analytics Val - October 2025 Login Analytics

This repository contains analytics dashboard and presentation for MyBambu login data for October 2025.

## 📊 Key Metrics

- **390,608** unique customers logged in during October 2025
- **3,386,334** total sessions
- **8.7** average sessions per user
- Nearly balanced iOS (49.8%) vs Android (47.7%) user base

## 📁 Files

### Visualizations
- **[october_2025_login_dashboard.html](october_2025_login_dashboard.html)** - Interactive dashboard with Chart.js visualizations
  - OS distribution charts
  - Daily activity trends
  - Platform version breakdown
  - Session distribution analysis

- **[october_2025_presentation.html](october_2025_presentation.html)** - Full-screen presentation (8 slides)
  - Scroll-snap navigation
  - Keyboard controls (arrow keys)
  - Key insights and metrics

### Data Analysis Scripts
- **[october_logins_by_os.py](october_logins_by_os.py)** - October 2025 OS breakdown analysis
- **[heap_sessions_detail.py](heap_sessions_detail.py)** - Session statistics for last 30 days
- **[heap_logins_query.py](heap_logins_query.py)** - Login queries across Heap tables

## 🚀 Usage

### View Locally
Open the HTML files directly in your browser:
```bash
open october_2025_login_dashboard.html
open october_2025_presentation.html
```

### Run Python Scripts
The Python scripts require Snowflake connector and appropriate credentials:
```bash
python3 october_logins_by_os.py
```

## 📈 Key Insights

- **Balanced Platform Appeal**: Nearly 50/50 iOS/Android split shows strong cross-platform strategy
- **High Engagement**: Android users are 24% more active with 9.7 avg sessions vs iOS 7.8
- **Mobile-First Success**: 97.6% of users on mobile platforms
- **Peak Activity**: October 3rd saw 143K sessions (highest of the month)

## 📊 Data Source

- Database: `MYBAMBU_PROD.BAMBU_MART_HEAP`
- Table: `MART_SESSIONS`
- Period: October 1-31, 2025
- Report Generated: November 14, 2025

## 🔐 Authentication

Scripts use Snowflake private key authentication. Ensure you have proper credentials configured before running the Python scripts.

---

Generated with [Claude Code](https://claude.com/claude-code)
