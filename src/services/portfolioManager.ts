// src/services/portfolioManager.ts
// Quản lý danh mục đầu tư nhiều coin dài hạn (Spot / Portfolio)
import { Portfolio, IPortfolio } from '../database/models/Portfolio';

/**
 * Lấy thông tin danh mục đầu tư của người dùng.
 * BẢO MẬT: Đã gỡ bỏ lệnh mongoose.connect hardcode.
 * Hệ thống tự động dùng kết nối Database sẵn có từ index.ts.
 */
export async function getPortfolio(user = 'default'): Promise<IPortfolio> {
    let pf = await Portfolio.findOne({ user });

    if (!pf) {
        pf = await Portfolio.create({
            user,
            positions: [],
            totalInvested: 0,
            totalUnrealizedPnl: 0,
            totalRealizedPnl: 0,
            updatedAt: new Date(),
        });
    }
    return pf;
}

/**
 * Thêm mới hoặc cập nhật vị thế vào danh mục
 * (Dùng khi mua thêm Spot: tính trung bình giá DCA)
 */
export async function upsertPosition(user: string, symbol: string, quantity: number, price: number, realizedPnl = 0) {
    const pf = await getPortfolio(user);
    let pos = pf.positions.find(p => p.symbol === symbol);

    if (!pos) {
        // Nếu chưa có coin này trong ví -> Thêm mới
        pos = {
            symbol,
            quantity,
            avgEntryPrice: price,
            invested: quantity * price,
            realizedPnl: 0,
            unrealizedPnl: 0,
            updatedAt: new Date(),
        };
        pf.positions.push(pos);
    } else {
        // Nếu đã có -> Cập nhật trung bình giá (Weighted Average) và số lượng
        const totalCost = (pos.avgEntryPrice * pos.quantity) + (price * quantity);
        pos.quantity += quantity;
        pos.avgEntryPrice = pos.quantity > 0 ? totalCost / pos.quantity : 0;
        pos.invested = pos.quantity * pos.avgEntryPrice;
        pos.updatedAt = new Date();
    }

    // Cập nhật lợi nhuận đã chốt
    pos.realizedPnl += realizedPnl;

    // Tổng hợp lại toàn bộ danh mục
    pf.totalInvested = pf.positions.reduce((acc, p) => acc + p.invested, 0);
    pf.totalRealizedPnl = pf.positions.reduce((acc, p) => acc + p.realizedPnl, 0);
    pf.updatedAt = new Date();

    await pf.save();
    return pf;
}

/**
 * Cập nhật lợi nhuận chưa chốt (Unrealized PnL) theo giá thị trường hiện tại
 * (Nên gọi định kỳ từ hệ thống cron/interval để dashboard luôn update)
 */
export async function updateUnrealizedPnl(user: string, symbol: string, currentPrice: number) {
    const pf = await getPortfolio(user);
    const pos = pf.positions.find(p => p.symbol === symbol);

    if (pos) {
        pos.unrealizedPnl = (currentPrice - pos.avgEntryPrice) * pos.quantity;
        pos.updatedAt = new Date();

        // Tổng hợp lại PnL chưa chốt của cả ví
        pf.totalUnrealizedPnl = pf.positions.reduce((acc, p) => acc + p.unrealizedPnl, 0);
        pf.updatedAt = new Date();
        await pf.save();
    }
    return pf;
}

/**
 * Xóa hoàn toàn một vị thế khỏi danh mục (Dùng khi đã chốt bán hết Spot)
 */
export async function removePosition(user: string, symbol: string) {
    const pf = await getPortfolio(user);

    // Lọc bỏ coin ra khỏi mảng
    pf.positions = pf.positions.filter(p => p.symbol !== symbol);

    // Cập nhật lại các thông số tổng
    pf.totalInvested = pf.positions.reduce((acc, p) => acc + p.invested, 0);
    pf.totalUnrealizedPnl = pf.positions.reduce((acc, p) => acc + p.unrealizedPnl, 0);
    pf.totalRealizedPnl = pf.positions.reduce((acc, p) => acc + p.realizedPnl, 0);
    pf.updatedAt = new Date();

    await pf.save();
    return pf;
}