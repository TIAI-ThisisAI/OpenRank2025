import React, { useState, useMemo, useEffect, useCallback } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend, LineChart, Line, AreaChart, Area,
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis
} from 'recharts';
import { 
  Globe, Clock, TrendingUp, HelpCircle, Sparkles, Activity, 
  Calendar, Map as MapIcon, Award, Users, Target, Clock4, 
  Filter, Download, XCircle, ChevronDown 
} from 'lucide-react';

// --- 配置与常量 ---
const API_KEY = ""; 
const API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent";

const TIME_RANGES = [
  { label: '近30天', value: '1M', days: 30 },
  { label: '近90天', value: '3M', days: 90 },
  { label: '近半年', value: '6M', days: 180 },
  { label: '近一年', value: '1Y', days: 365 },
];

const COLORS = ['#06b6d4', '#f59e0b', '#ec4899', '#8b5cf6', '#10b981', '#ef4444', '#f97316', '#a1a1aa'];
const COMMON_AXIS_PROPS = { stroke: "#9CA3AF", tickLine: false, axisLine: false, fontSize: 12 };
const TOOLTIP_STYLE = { backgroundColor: '#111827', border: '1px solid #374151', borderRadius: '8px', color: '#fff' };

// --- 动态数据生成器 ---
const generateMockData = (projectType, days) => {
  const now = Math.floor(Date.now() / 1000);
  const isA = projectType === 'Project-A';
  const count = isA ? days * 8 : days * 4;
  
  const commits = Array.from({ length: count }, () => {
    const ts = now - Math.floor(Math.random() * 86400 * days);
    const hour = new Date(ts * 1000).getUTCHours();
    let pool = isA ? ["USA", "CHN", "DEU", "IND", "JPN"] : ["USA", "GBR", "DEU", "CAN"];
    
    if (isA) {
      if (hour >= 1 && hour <= 9) pool = ["CHN", "IND", "JPN", "AUS"]; 
      else if (hour >= 9 && hour <= 17) pool = ["DEU", "GBR", "FRA"];
    }
    
    return {
      timestamp_unix: ts,
      location_iso3: pool[Math.floor(Math.random() * pool.length)],
      contributor_id: `u${Math.floor(Math.random() * (isA ? 50 : 15))}`,
      contributor_name: `Dev-${Math.floor(Math.random() * 1000)}`
    };
  }).sort((a, b) => a.timestamp_unix - b.timestamp_unix);

  const historyPoints = days > 90 ? 12 : 6;
  const history = Array.from({ length: historyPoints }, (_, i) => ({
    label: days > 180 ? `${i+1}月` : `W${i+1}`,
    score: 0.4 + Math.random() * 0.4,
    commits: Math.floor(Math.random() * 100) + 20
  }));

  const radarData = [
    { subject: '时区覆盖', A: isA ? 130 : 60 },
    { subject: '地域多元性', A: isA ? 120 : 40 },
    { subject: '贡献连续性', A: isA ? 110 : 90 },
    { subject: '社区活跃度', A: isA ? 140 : 80 },
    { subject: '新人留存', A: isA ? 95 : 60 },
    { subject: '非英语母语', A: isA ? 100 : 20 },
  ];

  return { commits, history, radarData };
};

// --- 子组件 ---
const ActivityHeatmap = ({ data }) => {
  const days = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];
  const getIntensity = (c) => c === 0 ? 'bg-gray-800/50' : c < 2 ? 'bg-cyan-900/40' : c < 5 ? 'bg-cyan-700/60' : c < 8 ? 'bg-cyan-500/80' : 'bg-cyan-400';

  return (
    <div className="flex flex-col h-full w-full overflow-x-auto select-none font-sans">
      <div className="flex mb-2">
        <div className="w-8" />
        <div className="flex-1 flex justify-between text-[10px] text-gray-500 px-1 font-mono">
          {[0, 3, 6, 9, 12, 15, 18, 21].map(h => <span key={h}>{h}H</span>)}
        </div>
      </div>
      {days.map((day, dIdx) => (
        <div key={day} className="flex items-center mb-1 group">
          <span className="w-8 text-[10px] text-gray-500 text-right pr-2 group-hover:text-cyan-400 transition-colors">{day}</span>
          <div className="flex-1 grid grid-cols-24 gap-[2px]">
            {Array.from({ length: 24 }).map((_, hIdx) => {
              const count = data[dIdx]?.[hIdx] || 0;
              return <div key={hIdx} className={`h-5 rounded-[2px] transition-all hover:scale-110 cursor-crosshair ${getIntensity(count)}`} title={`${day} ${hIdx}:00 - ${count}次提交`} />;
            })}
          </div>
        </div>
      ))}
    </div>
  );
};

const MetricCard = ({ title, value, icon: Icon, unit = '', color = 'text-cyan-400', subValue, info, loading }) => (
  <div className="bg-gray-800/40 backdrop-blur-md p-6 rounded-2xl border border-gray-700/50 hover:border-cyan-500/30 transition-all group">
    <div className="flex items-center justify-between mb-4">
      <h3 className="text-sm font-medium text-gray-400 flex items-center gap-1">
        {title} {info && <span className="text-gray-500 hover:text-cyan-400 cursor-help relative group/tip"><HelpCircle size={14} /><div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 invisible group-hover/tip:visible w-48 p-2 bg-gray-900 border border-gray-700 text-[10px] rounded shadow-xl z-50">{info}</div></span>}
      </h3>
      <div className={`p-2 rounded-xl bg-gray-800 border border-gray-700 ${color}`}><Icon size={18} /></div>
    </div>
    {loading ? <div className="animate-pulse h-12 bg-gray-700/50 rounded" /> : (
      <div>
        <p className="text-3xl font-bold text-gray-100">{value}<span className="text-lg text-gray-500 ml-1 font-normal">{unit}</span></p>
        {subValue && <p className="text-xs font-medium text-gray-500 uppercase mt-1">{subValue}</p>}
      </div>
    )}
  </div>
);

const ChartCard = ({ title, icon: Icon, children, className = "", subtitle, action }) => (
  <div className={`bg-gray-800/80 backdrop-blur rounded-2xl border border-gray-700 p-6 shadow-xl flex flex-col ${className}`}>
    <div className="mb-6 flex items-start justify-between">
      <div>
        <h3 className="text-lg font-bold text-gray-100 flex items-center gap-2"><Icon size={20} className="text-cyan-400" />{title}</h3>
        {subtitle && <p className="text-xs text-gray-500 mt-1">{subtitle}</p>}
      </div>
      {action}
    </div>
    <div className="flex-1 w-full min-h-0 relative">{children}</div>
  </div>
);

// --- 核心 Hook ---
const useDataProcessor = (project, range, filter) => {
  const rawData = useMemo(() => {
    const config = TIME_RANGES.find(r => r.value === range) || TIME_RANGES[0];
    return generateMockData(project, config.days);
  }, [project, range]);

  return useMemo(() => {
    const { commits, history, radarData } = rawData;
    const hourly = Array(24).fill(0), heatmap = Array(7).fill(0).map(() => Array(24).fill(0));
    const geo = {}, users = {};

    commits.forEach(c => {
      const d = new Date(c.timestamp_unix * 1000);
      const h = d.getUTCHours(), day = d.getUTCDay();
      hourly[h]++; heatmap[day][h]++;
      geo[c.location_iso3] = (geo[c.location_iso3] || 0) + 1;
      if (!users[c.contributor_id]) users[c.contributor_id] = { ...c, count: 0 };
      users[c.contributor_id].count++;
    });

    const hriData = hourly.map((c, h) => ({ hour: h.toString().padStart(2, '0'), commits: c }));
    const countryData = Object.entries(geo).sort((a, b) => b[1] - a[1]).map(([name, value]) => ({ name, value }));
    const pieData = [...countryData.slice(0, 5), ...(countryData.length > 5 ? [{ name: 'Others', value: countryData.slice(5).reduce((s, x) => s + x.value, 0) }] : [])];
    
    let filteredUsers = Object.values(users);
    if (filter) filteredUsers = filteredUsers.filter(u => u.location_iso3 === filter);
    
    const hriScore = hourly.filter(v => v > 0).length / 24;
    const geoScore = countryData.length > 0 ? Math.min(1, Math.log(countryData.length) / Math.log(8)) : 0;

    return {
      hriData, heatmapData: heatmap, radarData, history,
      geoData: { countryData, pieData, diversityScore: geoScore },
      totalCommits: commits.length,
      totalContributors: Object.keys(users).length,
      globalScore: (hriScore + geoScore) / 2,
      hriCoverage: Math.round(hriScore * 100),
      filteredContributors: filteredUsers.sort((a, b) => b.count - a.count).slice(0, 10),
      rawData
    };
  }, [rawData, filter]);
};

// --- 主应用 ---
export default function App() {
  const [project, setProject] = useState('Project-A');
  const [range, setRange] = useState('3M');
  const [filter, setFilter] = useState(null);
  const [loading, setLoading] = useState(true);
  const [insight, setInsight] = useState({ text: "", loading: false });

  const data = useDataProcessor(project, range, filter);

  useEffect(() => {
    setLoading(true); setFilter(null); setInsight({ text: "", loading: false });
    const t = setTimeout(() => setLoading(false), 500);
    return () => clearTimeout(t);
  }, [project, range]);

  const generateInsight = async () => {
    if (!API_KEY) return setInsight({ text: "请配置 API Key 启用 AI 分析。", loading: false });
    setInsight({ text: "", loading: true });
    try {
      const res = await fetch(`${API_URL}?key=${API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: `分析项目 ${project} 数据：得分${data.globalScore.toFixed(2)}, 覆盖率${data.hriCoverage}%` }] }],
          systemInstruction: { parts: [{ text: `你是一位开源专家。请分析数据并用 Markdown 格式给出：核心洞察、优势、风险、建议。语气专业直接。` }] },
        })
      });
      const resJson = await res.json();
      setInsight({ text: resJson.candidates?.[0]?.content?.parts?.[0]?.text || "分析失败", loading: false });
    } catch (e) {
      setInsight({ text: "API 调用异常", loading: false });
    }
  };

  const exportJson = () => {
    const blob = new Blob([JSON.stringify(data.rawData, null, 2)], { type: 'application/json' });
    const a = Object.assign(document.createElement('a'), { href: URL.createObjectURL(blob), download: `${project}_report.json` });
    a.click();
  };

  if (loading && !data) return <div className="min-h-screen bg-[#0B1120] flex items-center justify-center text-cyan-400">Loading...</div>;

  return (
    <div className="min-h-screen bg-[#0B1120] text-gray-100 font-sans pb-12">
      <nav className="border-b border-gray-800 bg-[#0B1120]/80 backdrop-blur-md sticky top-0 z-50 h-16 flex items-center px-6 justify-between">
        <div className="flex items-center gap-3">
          <div className="bg-gradient-to-br from-cyan-500 to-blue-600 w-8 h-8 rounded-lg flex items-center justify-center shadow-lg"><Globe size={18} className="text-white" /></div>
          <h1 className="text-lg font-bold">GlobalPulse <span className="text-cyan-500 font-medium">Analytics</span></h1>
        </div>
        <div className="flex items-center gap-4">
          <div className="bg-gray-900 p-1 rounded-lg border border-gray-700 flex">
            {TIME_RANGES.map(r => (
              <button key={r.value} onClick={() => setRange(r.value)} className={`px-3 py-1 text-xs rounded-md transition-all ${range === r.value ? 'bg-gray-700 text-cyan-400' : 'text-gray-500'}`}>{r.label}</button>
            ))}
          </div>
          <select value={project} onChange={e => setProject(e.target.value)} className="bg-gray-900 border border-gray-700 text-sm py-1.5 px-3 rounded-lg outline-none cursor-pointer">
            <option value="Project-A">Project-A</option>
            <option value="Project-B">Project-B</option>
          </select>
          <button onClick={exportJson} className="p-2 text-gray-400 hover:text-cyan-400 border border-gray-700 rounded-lg"><Download size={16} /></button>
        </div>
      </nav>

      <main className="max-w-[1600px] mx-auto p-6 space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <MetricCard title="全球化综合得分" value={data.globalScore.toFixed(2)} unit="/1.0" icon={Activity} subValue="OpenRank 综合指数" color={data.globalScore > 0.6 ? "text-emerald-400" : "text-amber-400"} loading={loading} />
          <MetricCard title="24HRI 覆盖率" value={data.hriCoverage} unit="%" icon={Clock} subValue="时区协作效率" loading={loading} />
          <MetricCard title="地理多样性" value={data.geoData.diversityScore.toFixed(2)} icon={MapIcon} subValue={`${data.geoData.countryData.length}个活跃地区`} color="text-purple-400" loading={loading} />
          <MetricCard title="独立贡献者" value={data.totalContributors} icon={Users} subValue={`总提交: ${data.totalCommits}`} color="text-pink-400" loading={loading} />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <ChartCard title="历史趋势" icon={TrendingUp} className="lg:col-span-2 h-[400px]">
            <ResponsiveContainer>
              <AreaChart data={data.history}>
                <defs><linearGradient id="cScore" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#06b6d4" stopOpacity={0.3}/><stop offset="95%" stopColor="#06b6d4" stopOpacity={0}/></linearGradient></defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false}/>
                <XAxis dataKey="label" {...COMMON_AXIS_PROPS} />
                <YAxis yAxisId="L" {...COMMON_AXIS_PROPS} domain={[0, 1]} />
                <YAxis yAxisId="R" orientation="right" {...COMMON_AXIS_PROPS} stroke="#8b5cf6" />
                <Tooltip contentStyle={TOOLTIP_STYLE} />
                <Area yAxisId="L" type="monotone" dataKey="score" stroke="#06b6d4" fill="url(#cScore)" />
                <Line yAxisId="R" type="monotone" dataKey="commits" stroke="#8b5cf6" strokeWidth={2} dot={{r:4}} />
              </AreaChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title="地域分布" icon={MapIcon} className="h-[400px]" action={filter && <button onClick={() => setFilter(null)} className="text-xs text-red-400 flex items-center gap-1"><XCircle size={12}/>重置</button>}>
            <ResponsiveContainer>
              <PieChart>
                <Pie data={data.geoData.pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={60} outerRadius={90} paddingAngle={5} onClick={d => d.name !== 'Others' && setFilter(d.name === filter ? null : d.name)} className="cursor-pointer">
                  {data.geoData.pieData.map((e, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} stroke={filter === e.name ? '#fff' : 'none'} fillOpacity={filter && filter !== e.name ? 0.4 : 1} />)}
                </Pie>
                <Tooltip contentStyle={TOOLTIP_STYLE} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title="协作脉冲" icon={Calendar} className="lg:col-span-2 h-[320px]"><ActivityHeatmap data={data.heatmapData} /></ChartCard>

          <ChartCard title="核心贡献者" icon={Award} subtitle={filter ? `地区: ${filter}` : "贡献量排名"} className="h-[320px]">
            <div className="overflow-y-auto h-full pr-2 text-sm custom-scrollbar">
              <table className="w-full text-left">
                <thead className="text-gray-500 border-b border-gray-700 sticky top-0 bg-gray-800"><tr className="text-xs"><th>Rank</th><th>User</th><th>Loc</th><th className="text-right">Commits</th></tr></thead>
                <tbody>
                  {data.filteredContributors.map((u, i) => (
                    <tr key={u.contributor_id} className="border-b border-gray-800/50 hover:bg-gray-700/30">
                      <td className="py-2 text-gray-500">{i+1}</td>
                      <td className="py-2 flex items-center gap-2"><div className="w-5 h-5 rounded-full bg-gray-700 flex items-center justify-center text-[10px]">{u.contributor_name[0]}</div>{u.contributor_name}</td>
                      <td><span className={`text-[10px] px-1 rounded border ${u.location_iso3 === filter ? 'border-cyan-500 text-cyan-400' : 'border-gray-700'}`}>{u.location_iso3}</span></td>
                      <td className="text-right font-mono text-cyan-400">{u.count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </ChartCard>

          <ChartCard title="健康度雷达" icon={Target} className="h-[350px]">
            <ResponsiveContainer><RadarChart data={data.radarData}><PolarGrid stroke="#374151"/><PolarAngleAxis dataKey="subject" tick={{fill:'#9CA3AF', fontSize:11}}/><Radar dataKey="A" stroke="#06b6d4" fill="#06b6d4" fillOpacity={0.3}/><Tooltip contentStyle={TOOLTIP_STYLE}/></RadarChart></ResponsiveContainer>
          </ChartCard>

          <ChartCard title="24小时活跃度" icon={Clock4} className="h-[350px]">
            <ResponsiveContainer>
              <AreaChart data={data.hriData}>
                <XAxis dataKey="hour" {...COMMON_AXIS_PROPS} tickFormatter={v => `${v}H`} />
                <Tooltip contentStyle={TOOLTIP_STYLE} />
                <Area type="monotone" dataKey="commits" stroke="#10b981" fill="#10b981" fillOpacity={0.2} />
              </AreaChart>
            </ResponsiveContainer>
          </ChartCard>
        </div>

        <div className="bg-gradient-to-r from-gray-900 to-gray-800 rounded-2xl p-8 border border-gray-700 relative overflow-hidden">
          <div className="absolute top-0 right-0 w-64 h-64 bg-cyan-500/5 blur-[100px] pointer-events-none" />
          <div className="flex flex-col lg:flex-row gap-8 relative z-10">
            <div className="lg:w-1/3 space-y-4">
              <h2 className="text-xl font-bold flex items-center gap-2"><Sparkles className="text-yellow-400" /> AI 智能诊断</h2>
              <p className="text-gray-400 text-sm leading-relaxed">深度解析当前时段数据，为您提供全球化运营建议与风险预警。</p>
              <button onClick={generateInsight} disabled={insight.loading} className="w-full py-3 bg-cyan-600 hover:bg-cyan-500 disabled:bg-gray-700 text-white font-bold rounded-xl transition-all shadow-lg flex items-center justify-center gap-2">
                {insight.loading ? <div className="w-4 h-4 border-2 border-white/30 border-t-white animate-spin rounded-full" /> : <Activity size={18} />}
                {insight.loading ? "分析中..." : `生成 ${project} 报告`}
              </button>
            </div>
            <div className="lg:w-2/3 bg-gray-900/50 rounded-xl p-6 border border-gray-800 min-h-[180px]">
              {insight.text ? <div className="prose prose-invert prose-sm max-w-none whitespace-pre-wrap text-gray-300 leading-7">{insight.text}</div> : (
                <div className="h-full flex flex-col items-center justify-center text-gray-600 italic text-sm"><Sparkles size={32} className="mb-2 opacity-20" /><p>点击按钮，AI 将为您深度解读数据模式...</p></div>
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
