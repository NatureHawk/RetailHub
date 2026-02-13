// src/App.jsx
import React, { useState, useEffect, useMemo } from 'react';
import axios from 'axios';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend
} from 'recharts';

// --- API CONFIGURATION ---
const API_BASE = "http://127.0.0.1:8000";

const PIE_COLORS = ['#2B4ACB', '#93a9ff'];

// --- NAVIGATION ---
const Navigation = () => {
  const location = useLocation();
  const activeStyle = { borderBottom: '3px solid #2B4ACB' };
  return (
    <nav className="navbar">
      <Link to="/" className="nav-link" style={location.pathname === '/' ? activeStyle : {}}>Overview</Link>
      <Link to="/commercial" className="nav-link" style={location.pathname === '/commercial' ? activeStyle : {}}>Commercial</Link>
      <Link to="/operations" className="nav-link" style={location.pathname === '/operations' ? activeStyle : {}}>Operations</Link>
      <Link to="/customer" className="nav-link" style={location.pathname === '/customer' ? activeStyle : {}}>Customer</Link>
    </nav>
  );
};

// --- PAGE 1: OVERVIEW ---
// --- PAGE 1: OVERVIEW ---
const Overview = () => {
  const [dateRange, setDateRange] = useState('All Time'); // Changed default to All Time
  const [year, setYear] = useState('All Years');          // NEW Year State
  const [city, setCity] = useState('All Cities');
  const [liveData, setLiveData] = useState([]);
  const [kpis, setKpis] = useState({ revenue: "$0", orders: "0", growth: "0%" });

  // Wiring the dropdowns to the backend
  useEffect(() => {
    const fetchOverviewData = async () => {
      try {
        const res = await axios.get(`${API_BASE}/analytics/filter`, {
          // Pass the new year parameter to the backend
          params: { period: dateRange, city: city, year: year }
        });

        setLiveData(res.data.revenue_chart);

        const totalRev = res.data.revenue_chart.reduce((acc, curr) => acc + curr.revenue, 0);
        const totalOrders = res.data.top_products.reduce((acc, curr) => acc + curr.sales, 0);

        setKpis({
          revenue: `$${(totalRev / 1000).toFixed(1)}K`,
          orders: totalOrders,
          growth: "85%"
        });
      } catch (e) {
        console.error("API Error", e);
      }
    };
    fetchOverviewData();
  }, [dateRange, city, year]); // Added year to dependency array

  return (
    <div className="container">
      <div className="header">
        <div className="logo">PINGU</div>
        <div className="controls">
          <select className="dropdown" value={dateRange} onChange={(e) => setDateRange(e.target.value)}>
            <option>All Time</option>
            <option>Last 30 Days</option>
            <option>Last 6 Months</option>
            <option>Last Year</option>
          </select>

          {/* NEW YEAR DROPDOWN */}
          <select className="dropdown" value={year} onChange={(e) => setYear(e.target.value)}>
            <option>All Years</option>
            <option>2026</option>
            <option>2025</option>
            <option>2024</option>
            <option>2023</option>
            <option>2022</option>
            <option>2021</option>
          </select>

          <select className="dropdown" value={city} onChange={(e) => setCity(e.target.value)}>
            <option>All Cities</option>
            <option>New York</option>
            <option>Los Angeles</option>
            <option>Chicago</option>
            <option>Houston</option>
            <option>Miami</option>
            <option>Seattle</option>
            <option>Atlanta</option>
            <option>San Francisco</option>
          </select>
        </div>
      </div>
      <h2 className="section-title">OVERVIEW</h2>

      <div className="kpi-container">
        <div className="kpi-card">
          <div className="kpi-badge">{kpis.revenue}</div>
          <div className="kpi-label">TOTAL REVENUE</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-badge">{kpis.orders}</div>
          <div className="kpi-label">TOTAL ORDERS</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-badge">{kpis.growth}</div>
          <div className="kpi-label">GROWTH RATE</div>
        </div>
      </div>

      <div className="graph-section">
        <div className="graph-box">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={liveData}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#ccc" />
              <XAxis dataKey="name" stroke="#2B4ACB" />
              <YAxis stroke="#2B4ACB" />
              <Tooltip cursor={{ fill: '#f0f0f0' }} contentStyle={{ borderRadius: '8px', border: '2px solid #2B4ACB' }} />
              <Bar dataKey="revenue" fill="#2B4ACB" radius={[4, 4, 0, 0]} barSize={40} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
      <Navigation />
    </div>
  );
};
// --- PAGE 2: COMMERCIAL ---
const Commercial = () => {
  const [selectedCity, setSelectedCity] = useState("All Cities");
  const [data, setData] = useState({ chart: [], products: [] });

  useEffect(() => {
    axios.get(`${API_BASE}/analytics/commercial`).then(res => {
      setData({
        chart: res.data.monthly_revenue.map(r => ({ name: r.month, revenue: r.revenue })),
        products: res.data.top_products.map(p => ({ name: p.name, sales: p.sold, revenue: `$${(p.sold * 45).toLocaleString()}` }))
      });
    });
  }, []);

  return (
    <div className="container">
      <div className="header"><div className="logo">PINGU</div></div>
      <h2 className="section-title">COMMERCIAL ANALYTICS</h2>
      <div className="split-layout">
        <div className="panel-left">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
            <h3 style={{ margin: 0, color: '#2B4ACB', fontSize: '1.2rem' }}>MONTHLY REVENUE</h3>
            <select className="dropdown" value={selectedCity} onChange={(e) => setSelectedCity(e.target.value)}>
              <option>All Cities</option>
              <option>New York</option>
              <option>London</option>
            </select>
          </div>
          <div className="chart-container" style={{ border: 'none', padding: 0 }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.chart}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#ccc" />
                <XAxis dataKey="name" stroke="#2B4ACB" />
                <YAxis stroke="#2B4ACB" />
                <Tooltip cursor={{ fill: '#f0f0f0' }} contentStyle={{ borderRadius: '8px', border: '2px solid #2B4ACB' }} />
                <Bar dataKey="revenue" fill="#2B4ACB" radius={[4, 4, 0, 0]} barSize={40} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="panel-right">
          <h3 style={{ margin: '0 0 15px 0', color: '#2B4ACB', textAlign: 'center', borderBottom: '2px solid #2B4ACB', paddingBottom: '10px', fontSize: '1.2rem' }}>TOP PRODUCTS</h3>
          <div style={{ display: 'flex', flexDirection: 'column' }}>
            {data.products.map((product, index) => (
              <div key={index} className="product-item">
                <div className="product-rank">#{index + 1}</div>
                <div className="product-info"><div className="product-name">{product.name}</div><div className="product-sales">{product.sales} sold</div></div>
                <div className="product-revenue">{product.revenue}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
      <Navigation />
    </div>
  );
};

// --- PAGE 3: OPERATIONS ---
const CATEGORY_COLORS = { Food: '#2B4ACB', Home: '#C07850', 'Personal Care': '#7A9E7E', Other: '#A8A095' };

const Operations = () => {
  const [data, setData] = useState({ seasonalByCategory: [], turnover: 0, delivery: 0 });

  useEffect(() => {
    axios.get(`${API_BASE}/analytics/operations`).then(res => {
      setData({
        seasonalByCategory: res.data.seasonal_by_category || [],
        turnover: res.data.inventory_turnover,
        delivery: res.data.avg_delivery_days
      });
    });
  }, []);

  // Discover all category keys from the data (excluding 'name')
  const categories = useMemo(() => {
    const keys = new Set();
    data.seasonalByCategory.forEach(entry => {
      Object.keys(entry).forEach(k => { if (k !== 'name') keys.add(k); });
    });
    return Array.from(keys);
  }, [data.seasonalByCategory]);

  return (
    <div className="container">
      <div className="header"><div className="logo">PINGU</div></div>
      <h2 className="section-title">OPERATIONS & INVENTORY</h2>
      <div className="kpi-container">
        <div className="kpi-card">
          <div className="kpi-badge">{data.turnover}</div>
          <div className="kpi-label">INVENTORY TURNOVER RATIO</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-badge">{data.delivery} Days</div>
          <div className="kpi-label">AVERAGE DELIVERY TIME</div>
        </div>
      </div>
      <div className="graph-section">
        <h3 style={{ color: '#2B4ACB', marginBottom: '5px', fontSize: '1rem', textAlign: 'center' }}>
          SEASONAL DEMAND BY CATEGORY
        </h3>
        <div className="graph-box">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data.seasonalByCategory}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#ccc" />
              <XAxis dataKey="name" stroke="#2B4ACB" />
              <YAxis stroke="#2B4ACB" />
              <Tooltip cursor={{ fill: '#f0f0f0' }} contentStyle={{ borderRadius: '8px', border: '2px solid #2B4ACB' }} />
              <Legend />
              {categories.map(cat => (
                <Bar key={cat} dataKey={cat} stackId="a" fill={CATEGORY_COLORS[cat] || '#94a3b8'} radius={cat === categories[categories.length - 1] ? [4, 4, 0, 0] : [0, 0, 0, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
      <Navigation />
    </div>
  );
};

// --- PAGE 4: CUSTOMER ---
// --- PAGE 4: CUSTOMER ---
const Customer = () => {
  const [data, setData] = useState({ retention: [], clvTrend: [], basket: [] });

  useEffect(() => {
    axios.get(`${API_BASE}/analytics/customer`).then(res => {
      setData({
        retention: res.data.retention,
        clvTrend: res.data.clv_trend,
        basket: res.data.market_basket.map(m => ({ pair: m.pair, count: m.count }))
      });
    });
  }, []);

  return (
    <div className="container">
      <div className="header"><div className="logo">PINGU</div></div>
      <h2 className="section-title">CUSTOMER INSIGHTS</h2>

      <div className="split-layout" style={{ background: 'transparent', border: 'none', padding: 0 }}>

        {/* LEFT COLUMN (Stacked Charts) */}
        <div style={{ flex: 2, display: 'flex', flexDirection: 'column', gap: '10px', minHeight: 0 }}>

          {/* 1. PIE CHART */}
          <div style={{ flex: 1, background: 'white', border: '2px solid #2B4ACB', padding: '10px 15px', display: 'flex', flexDirection: 'column', minHeight: 0, overflow: 'hidden' }}>
            <h3 style={{ margin: '0 0 5px 0', color: '#2B4ACB', fontSize: '0.9rem', textTransform: 'uppercase', flexShrink: 0 }}>NEW VS RETURNING</h3>
            <div style={{ flexGrow: 1, minHeight: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={data.retention}
                    innerRadius={50}
                    outerRadius={70}
                    paddingAngle={5}
                    dataKey="value"
                  >
                    {data.retention.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={PIE_COLORS[index % PIE_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip />
                  <Legend verticalAlign="middle" align="right" layout="vertical" iconType="square" />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* 2. CLV LINE CHART */}
          <div style={{ flex: 1, background: 'white', border: '2px solid #2B4ACB', padding: '10px 15px', display: 'flex', flexDirection: 'column', minHeight: 0, overflow: 'hidden' }}>
            <h3 style={{ margin: '0 0 5px 0', color: '#2B4ACB', fontSize: '0.9rem', textTransform: 'uppercase', flexShrink: 0 }}>AVG CUSTOMER LIFETIME VALUE (TREND)</h3>
            <div style={{ flexGrow: 1, minHeight: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data.clvTrend}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#ccc" />
                  <XAxis dataKey="month" stroke="#2B4ACB" />
                  <YAxis stroke="#2B4ACB" domain={['auto', 'auto']} />
                  <Tooltip contentStyle={{ borderRadius: '8px', border: '2px solid #2B4ACB' }} />
                  <Line
                    type="monotone"
                    dataKey="value"
                    stroke="#2B4ACB"
                    strokeWidth={3}
                    dot={{ r: 3, fill: '#fff', stroke: '#2B4ACB', strokeWidth: 2 }}
                    activeDot={{ r: 5 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* RIGHT PANEL: MARKET BASKET */}
        <div className="panel-right">
          <h3 style={{ margin: '0 0 10px 0', color: '#2B4ACB', textAlign: 'center', borderBottom: '2px solid #2B4ACB', paddingBottom: '8px', fontSize: '1rem' }}>
            MARKET BASKET
          </h3>
          <p style={{ fontSize: '0.75rem', textAlign: 'center', color: '#666', marginBottom: '10px' }}>
            Items frequently bought together
          </p>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            {(() => {
              const maxCount = data.basket.length > 0 ? Math.max(...data.basket.map(c => c.count)) : 1;
              return data.basket.map((combo, index) => (
                <div key={index} style={{ borderBottom: '1px dashed #eee', paddingBottom: '8px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                    <span style={{ fontWeight: 'bold', color: '#333', fontSize: '0.85rem' }}>#{index + 1} {combo.pair}</span>
                    <span style={{ fontSize: '0.75rem', color: '#2B4ACB', fontWeight: 'bold' }}>{combo.count}x</span>
                  </div>

                  <div style={{ background: '#f0f4ff', borderRadius: '4px', overflow: 'hidden', height: '20px' }}>
                    <div style={{
                      width: `${(combo.count / maxCount) * 100}%`,
                      background: 'linear-gradient(90deg, #2B4ACB, #93a9ff)',
                      height: '100%',
                      borderRadius: '4px',
                      transition: 'width 1s ease-in-out'
                    }}></div>
                  </div>
                </div>
              ));
            })()}
          </div>
        </div>
      </div>
      <Navigation />
    </div>
  );
};

// --- ROUTER ---
function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Overview />} />
        <Route path="/commercial" element={<Commercial />} />
        <Route path="/operations" element={<Operations />} />
        <Route path="/customer" element={<Customer />} />
      </Routes>
    </Router>
  );
}

export default App;