# Poll Winner

אפליקציית סקרים ציבורית עם:

- יצירת סקר עם כמה תשובות
- שיתוף באמצעות QR
- הצבעה עם שם
- הגדרת admin להצבעה אחת לכל שם
- תוצאות בזמן אמת
- בחירת מנצח אוטומטית לפי הכי הרבה הצבעות
- ממשק עברית/אנגלית

## פריסה לאינטרנט

הפרויקט מוכן לפריסה כשירות Node ציבורי. אחרי הפריסה, נכנסים לדומיין של השירות, למשל:

```text
https://your-poll-site.example
```

ה-QR שה-admin מקבל יפנה אוטומטית לדומיין הציבורי.

## Render

אפשר לפרוס דרך Render בעזרת `render.yaml`.

הגדרות השירות:

- Build Command: אין צורך
- Start Command: `node server.js`
- Environment: `Node`
- Persistent Disk: נדרש כדי שהסקרים יישמרו אחרי restart
- `DATA_FILE`: `/var/data/data.json`

## Docker

אפשר לפרוס בכל שירות שתומך Docker:

```bash
docker build -t poll-winner .
docker run -p 3000:3000 -e DATA_FILE=/data/data.json -v poll_winner_data:/data poll-winner
```

## קבצים מרכזיים

- `server.js` - שרת API, שמירת נתונים, הצבעות ועדכוני זמן אמת
- `public/index.html` - הממשק
- `public/app.js` - לוגיקת UI
- `public/qr.js` - יצירת QR מקומית ללא שירות חיצוני
