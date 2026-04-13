const express = require('express');
const cors = require('cors');
const PaperAccount = require('../models/PaperAccount');
const ActiveRoster = require('../models/ActiveRoster');

const app = express();
app.use(cors());
app.use(express.json());

app.get('/api/account', async (req, res) => {
    try {
        const account = await PaperAccount.findOne({ account_id: 'main_paper' });
        res.json(account);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

const startServer = () => {
    app.listen(3000, () => {
        console.log(`📡 [API SERVER] Dashboard ready at http://localhost:3000`);
    });
};

module.exports = { startServer };