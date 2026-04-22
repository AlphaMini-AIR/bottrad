/**
 * src/services/execution/ReportEngine.js
 * Nhiệm vụ: Quét toàn bộ các cụm DB để báo cáo hiệu suất giao dịch.
 */
const { getDbConnection } = require('../../config/db');
const PaperTradeSchema = require('../../models/PaperTrade');

async function getSimpleReport() {
    const nodes = ['tier1', 'tier2', 'scout'];
    let totalTrades = 0;
    let wins = 0;
    let totalNetPnl = 0;

    console.log('\n📊 --- BÁO CÁO CHIẾN TRƯỜNG AI ---');

    for (const node of nodes) {
        const conn = getDbConnection(node);
        const TradeModel = conn.model('PaperTrade', PaperTradeSchema);
        
        const trades = await TradeModel.find({});
        const nodePnl = trades.reduce((sum, t) => sum + t.netPnl, 0);
        const nodeWins = trades.filter(t => t.outcome === 'WIN').length;

        totalTrades += trades.length;
        wins += nodeWins;
        totalNetPnl += nodePnl;

        if (trades.length > 0) {
            console.log(`Node ${node.toUpperCase()}: ${trades.length} lệnh | PnL: ${nodePnl.toFixed(4)}$`);
        }
    }

    const winRate = totalTrades > 0 ? (wins / totalTrades * 100).toFixed(2) : 0;

    console.log('---------------------------------');
    console.log(`🔥 Tổng lệnh: ${totalTrades}`);
    console.log(`📈 Tỉ lệ thắng: ${winRate}%`);
    console.log(`💰 Tổng PnL: ${totalNetPnl.toFixed(4)}$`);
    console.log('---------------------------------\n');
}

module.exports = { getSimpleReport };