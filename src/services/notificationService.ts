// notificationService.ts - Gửi thông báo Telegram/email khi có lệnh hoặc cảnh báo rủi ro
import fetch from 'node-fetch';

// ========== TELEGRAM ==========
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';

export async function sendTelegramMessage(message: string) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message }),
  });
}

// ========== EMAIL (Gmail SMTP placeholder) ==========
import nodemailer from 'nodemailer';

const EMAIL_USER = process.env.EMAIL_USER || '';
const EMAIL_PASS = process.env.EMAIL_PASS || '';
const EMAIL_TO = process.env.EMAIL_TO || '';

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: { user: EMAIL_USER, pass: EMAIL_PASS },
});

export async function sendEmail(subject: string, text: string) {
  if (!EMAIL_USER || !EMAIL_PASS || !EMAIL_TO) return;
  await transporter.sendMail({ from: EMAIL_USER, to: EMAIL_TO, subject, text });
}
