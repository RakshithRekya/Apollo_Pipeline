# 🚀 Apollo Data Enrichment Pipeline

This is a fully automated Python workflow that:
1. Scrapes contact data from Apollo using Apify
2. Verifies personal emails using the Bounceban API
3. Enriches profiles by scraping LinkedIn using Apify again
4. Fills missing emails between data sources
5. Compares company names for consistency
6. Fixes incomplete last names using LinkedIn data

All steps run via a CLI interface with input prompts and can be executed individually or end-to-end.

---

## 🔧 Features

- ✅ Modularized Python functions (step1 to step6)
- ✅ Runs from terminal using a clean CLI menu
- ✅ API keys and Apollo URL are entered at runtime for flexibility
- ✅ Asynchronous email validation with Bounceban
- ✅ Automatic Excel output with color-coded highlights
- ✅ Git version control ready
- ✅ Ready to be Dockerized or turned into a lightweight GUI

---

## 📁 File Structure

apollo_pipeline/ ├── apollo_pipeline.py # Main script with all logic and CLI 
                 ├── apollo_scraped_data.xlsx # Auto-generated Excel with all results 
                 ├── README.md 

---
## 📬 Author 
Rakshith Rekya

