# Poll Winner Streamlit

This repository now includes a Streamlit version of the Poll Winner app.

Deploy it on Streamlit Community Cloud with:

- Repository: `coralfridman/BudgetKeyAnalyzer`
- Branch: `main`
- Main file path: `poll_winner_streamlit.py`

The app supports:

- Poll creation
- Share links and QR code
- Voting with voter names
- Admin settings
- One vote per name
- Multiple-choice polls
- Winner calculation by most votes
- Hebrew / English UI
- Auto-refreshing results while the page is open

Note: Streamlit Community Cloud file storage can reset when the app restarts. For long-term production persistence, connect the app to a hosted database such as Supabase, Neon, or Firebase.
