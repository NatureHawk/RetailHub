import { useState, useEffect } from 'react';
import axios from 'axios';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

function App() {
  const [kpi, setKpi] = useState({ total_revenue: 0, total_orders: 0, low_stock_alerts: 0 });
  const [chartData, setChartData] = useState([]);
  const [loading, setLoading] = useState(false);

  // 1. FETCH DATA FROM YOUR PYTHON BACKEND
  const fetchData = async () => {
    try {
      // Fetch KPIs
      const kpiRes = await axios.get('http://127.0.0.1:8000/kpi/summary');
      setKpi(kpiRes.data);

      // Fetch Chart Data
      const chartRes = await axios.get('http://127.0.0.1:8000/kpi/revenue-trend');
      setChartData(chartRes.data);
    } catch (error) {
      console.error("Error connecting to Python backend:", error);
    }
  };

  // 2. SIMULATE SALE (THE MAGIC BUTTON)
  const simulateSale = async () => {
    setLoading(true);
    try {
      await axios.post('http://127.0.0.1:8000/simulate/new-sale');
      await fetchData(); // Refresh data immediately
      alert("💰 New Sale Simulated!");
    } catch (error) {
      console.error("Error simulating sale:", error);
    }
    setLoading(false);
  };

  // Load data on start
  useEffect(() => {
    fetchData();
  }, []);

  // --- THE UI ---
  return (
    <div style={{ padding: '20px', fontFamily: 'Arial, sans-serif', maxWidth: '800px', margin: '0 auto' }}>
      <h1>🚀 Smart Retail Data Hub</h1>

      {/* KPI CARDS */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '20px', marginBottom: '40px' }}>
        <Card title="Total Revenue" value={`$${kpi.total_revenue}`} color="#d1fae5" textColor="#065f46" />
        <Card title="Total Orders" value={kpi.total_orders} color="#dbeafe" textColor="#1e40af" />
        <Card title="Low Stock Alerts" value={kpi.low_stock_alerts} color="#fee2e2" textColor="#991b1b" />
      </div>

      {/* REVENUE CHART */}
      <h2>Revenue Trends (Last 7 Days)</h2>
      <div style={{ height: '300px', marginBottom: '30px', border: '1px solid #ddd', padding: '10px', borderRadius: '8px' }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="day" />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="revenue" stroke="#8884d8" strokeWidth={3} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* ACTION BUTTON */}
      <button
        onClick={simulateSale}
        disabled={loading}
        style={{
          padding: '15px 30px',
          fontSize: '18px',
          backgroundColor: loading ? '#ccc' : '#10b981',
          color: 'white',
          border: 'none',
          borderRadius: '5px',
          cursor: loading ? 'not-allowed' : 'pointer'
        }}
      >
        {loading ? 'Processing...' : '💸 Simulate New Sale'}
      </button>
    </div>
  );
}

// Simple Card Component
const Card = ({ title, value, color, textColor }) => (
  <div style={{ backgroundColor: color, padding: '20px', borderRadius: '10px', textAlign: 'center' }}>
    <h3 style={{ color: textColor, margin: '0 0 10px 0' }}>{title}</h3>
    <p style={{ fontSize: '24px', fontWeight: 'bold', margin: 0, color: textColor }}>{value}</p>
  </div>
);

export default App;