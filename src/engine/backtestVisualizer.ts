// backtestVisualizer.ts - Visualization cho kết quả backtest (Node.js, xuất HTML)
import fs from 'fs';

export function generateEquityCurveHtml(equityCurve: number[], filePath: string) {
  const html = `
  <html>
    <head>
      <title>Equity Curve</title>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body>
      <canvas id="equityChart" width="800" height="400"></canvas>
      <script>
        const ctx = document.getElementById('equityChart').getContext('2d');
        const chart = new Chart(ctx, {
          type: 'line',
          data: {
            labels: Array.from({length: ${equityCurve.length}}, (_, i) => i),
            datasets: [{
              label: 'Equity',
              data: ${JSON.stringify(equityCurve)},
              borderColor: 'blue',
              fill: false
            }]
          },
          options: { responsive: false }
        });
      </script>
    </body>
  </html>
  `;
  fs.writeFileSync(filePath, html);
}
