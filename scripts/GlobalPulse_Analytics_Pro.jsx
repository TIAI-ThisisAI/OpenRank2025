import React, { useState, useMemo, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend, AreaChart, Area,
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis
} from 'recharts';
import { 
  Globe, TrendingUp, Sparkles, Activity, 
  Calendar, Map as MapIcon, Award, Users, Target, 
  Download, XCircle, Zap, LayoutDashboard
} from 'lucide-react';

/* =============================================================================
   MODULE 1: 配置与常量定义 (Configuration & Constants)
   作用：集中管理全局使用的静态数据、API端点、颜色主题和图表样式。
   ============================================================================= */

const APP_CONFIG = {
  apiKey: "", // 请在此处填入 Google Gemini API Key
  apiUrl: "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent"
};

const TIME_RANGES = [
  { label: '30D', value: '1M', days: 30 },
  { label: '90D', value: '3M', days: 90 },
  { label: '180D', value: '6M', days: 180 },
  { label: '1Y', value: '1Y', days: 365 },
];

const THEME = {
  colors: ['#06b6d4', '#8b5cf6', '#f43f5e', '#f59e0b', '#10b981', '#3b82f6'],
  chart: {
    axis: { stroke: "#64748b", tickLine: false, axisLine: false, fontSize: 11, fontFamily: 'monospace' },
    grid: { stroke: "#1e293b", strokeDasharray: "3 3" },
    tooltip: { 
      backgroundColor: 'rgba(15, 23, 42, 0.9)', 
      border: '1px solid rgba(56, 189, 248, 0.2)', 
      borderRadius: '8px', 
      boxShadow: '0 4px 20px rgba(0,0,0,0.5)',
      color: '#e2e8f0',
      fontSize: '12px'
    }
  }
};

/* =============================================================================
   MODULE 2: 数据模拟服务 (Mock Data Service)
   作用：负责生成虚拟的 Commits 数据、历史趋势数据和雷达图数据。
   包含模拟时区分布和活跃度的核心算法。
   ============================================================================= */

/**
 * 生成符合指定项目类型和时间跨度的模拟数据
 * @param {string} projectType - 项目标识 (Project-A/B/C)
 * @param {number} days - 时间跨度天数
 */
const DataService = {
  generate: (projectType, days) => {
    const now = Math.floor(Date.now() / 1000);
    const isA = projectType === 'Project-A';
    const count = isA ? days * 12 : days * 6;
    
    // 生成原始 Commit 列表
    const commits = Array.from({ length: count }, () => {
      const ts = now - Math.floor(Math.random() * 86400 * days);
      const hour = new Date(ts * 1000).getUTCHours();
      
      // 模拟地理分布逻辑
      let pool = isA ? ["USA", "CHN", "DEU", "IND", "JPN"] : ["USA", "GBR", "DEU", "CAN"];
      
      // 模拟时区活跃逻辑
      if (isA) {
        if (hour >= 2 && hour <= 10) pool = ["CHN", "IND", "JPN", "AUS"];
        else if (hour >= 10 && hour <= 18) pool = ["DEU", "GBR", "FRA", "NLD"];
      }
      
      return {
        timestamp_unix: ts,
        location_iso3: pool[Math.floor(Math.random() * pool.length)],
        contributor_id: `u${Math.floor(Math.random() * (isA ? 80 : 30))}`,
        contributor_name: `Dev-${Math.floor(Math.random() * 9000) + 1000}`
      };
    }).sort((a, b) => a.timestamp_unix - b.timestamp_unix);

    // 生成趋势图数据
    const historyPoints = days > 90 ? 20 : 10;
    const history = Array.from({ length: historyPoints }, (_, i) => ({
      label: days > 180 ? `${i+1}M` : `W${i+1}`,
      score: 0.5 + Math.random() * 0.4,
      commits: Math.floor(Math.random() * 150) + 40,
      contributors: Math.floor(Math.random() * 50) + 10
    }));

    // 生成雷达图数据
    const radarData = [
      { subject: 'Timezone', A: isA ? 130 : 60, full: 150 },
      { subject: 'Diversity', A: isA ? 120 : 40, full: 150 },
      { subject: 'Consistency', A: isA ? 110 : 90, full: 150 },
      { subject: 'Activity', A: isA ? 140 : 80, full: 150 },
      { subject: 'Retention', A: isA ? 95 : 60, full: 150 },
      { subject: 'Language', A: isA ? 100 : 20, full: 150 },
    ];

    return { commits, history, radarData };
  }
};

/* =============================================================================
   MODULE 3: 业务逻辑挂钩 (Custom Hooks / Logic Layer)
   作用：数据转换器。接收原始数据，转换为 UI 组件（热力图、饼图、列表）所需的特定格式。
   包含数据聚合、过滤和评分计算逻辑。
   ============================================================================= */

const useDataProcessor = (project, range, filter) => {
  // 1. 获取原始数据 (缓存结果)
  const rawData = useMemo(() => {
    const config = TIME_RANGES.find(r => r.value === range) || TIME_RANGES[0];
    return DataService.generate(project, config.days);
  }, [project, range]);

  // 2. 数据处理与聚合
  return useMemo(() => {
    const { commits, history, radarData } = rawData;
    
    // 初始化统计容器
    const hourly = Array(24).fill(0); 
    const heatmap = Array(7).fill(0).map(() => Array(24).fill(0));
    const geo = {}; 
    const users = {};

    // 遍历聚合
    commits.forEach(c => {
      const d = new Date(c.timestamp_unix * 1000);
      const h = d.getUTCHours(), day = d.getUTCDay();
      
      hourly[h]++; 
      heatmap[day][h]++;
      geo[c.location_iso3] = (geo[c.location_iso3] || 0) + 1;
      
      if (!users[c.contributor_id]) users[c.contributor_id] = { ...c, count: 0 };
      users[c.contributor_id].count++;
    });

    // 注入模拟的"人类行为模式" (工作时间/周末) - 为了热力图好看
    const enhancePattern = (arr2d, arr1d) => {
      // 工作日高峰
      for (let d = 1; d <= 5; d++) {
        [[9,11], [14,17]].forEach(([s, e]) => {
          for (let h = s; h <= e; h++) {
             const val = Math.floor(Math.random() * 8) + 5;
             arr2d[d][h] += val;
             arr1d[h] += val;
          }
        });
        // 随机加班
        if (Math.random() > 0.5) {
           for (let h = 20; h <= 22; h++) { 
             const val = Math.floor(Math.random() * 4) + 1;
             arr2d[d][h] += val; arr1d[h] += val;
           }
        }
      }
      // 周末稀疏
      [0, 6].forEach(d => {
        if (Math.random() > 0.7) {
          for (let h = 14; h <= 16; h++) {
             const val = Math.floor(Math.random() * 3) + 1;
             arr2d[d][h] += val; arr1d[h] += val;
          }
        }
      });
    };
    enhancePattern(heatmap, hourly);

    // 格式化输出数据
    const countryData = Object.entries(geo).sort((a, b) => b[1] - a[1]).map(([name, value]) => ({ name, value }));
    const pieData = [...countryData.slice(0, 5), ...(countryData.length > 5 ? [{ name: 'Other', value: countryData.slice(5).reduce((s, x) => s + x.value, 0) }] : [])];
    
    let filteredUsers = Object.values(users);
    if (filter) filteredUsers = filteredUsers.filter(u => u.location_iso3 === filter);
    
    const hriScore = hourly.filter(v => v > 0).length / 24;
    const geoScore = countryData.length > 0 ? Math.min(1, Math.log(countryData.length) / Math.log(8)) : 0;

    return {
      heatmapData: heatmap, 
      radarData, 
      history,
      geoData: { countryData, pieData, diversityScore: geoScore },
      totalCommits: commits.length, 
      totalContributors: Object.keys(users).length,
      globalScore: (hriScore + geoScore) / 2,
      hriCoverage: Math.round(hriScore * 100),
      filteredContributors: filteredUsers.sort((a, b) => b.count - a.count).slice(0, 20),
      rawData 
    };
  }, [rawData, filter]);
};

/* =============================================================================
   MODULE 4: UI 组件库 (UI Component Library)
   作用：通用的展示组件，负责具体的视觉渲染。
   包括：指标卡片、图表容器、热力图组件。
   ============================================================================= */

// 4.1 通用指标卡片
const MetricCard = ({ title, value, icon: Icon, unit, subValue, change, color }) => (
  <div className="relative overflow-hidden bg-slate-900/60 backdrop-blur-md p-5 rounded-2xl border border-slate-800 group hover:border-slate-700 transition-all duration-300 hover:shadow-lg hover:shadow-cyan-900/10 h-full">
    <div className={`absolute top-0 right-0 p-3 opacity-10 group-hover:opacity-20 transition-opacity ${color}`}>
      <Icon size={64} />
    </div>
    <div className="flex flex-col justify-between h-full relative z-10">
      <div className="flex items-center gap-2 text-slate-400 mb-1">
        <Icon size={16} />
        <span className="text-xs font-bold tracking-wider uppercase">{title}</span>
      </div>
      <div className="flex items-baseline gap-1">
        <span className="text-3xl font-black text-slate-100 tracking-tight">{value}</span>
        {unit && <span className="text-sm text-slate-500 font-medium">{unit}</span>}
      </div>
      {subValue && (
        <div className="mt-2 flex items-center gap-2 text-xs">
          <span className={`px-1.5 py-0.5 rounded bg-slate-800 border border-slate-700 ${change >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
            {change > 0 ? '+' : ''}{change}%
          </span>
          <span className="text-slate-500">{subValue}</span>
        </div>
      )}
    </div>
  </div>
);

// 4.2 图表包装容器 (处理 Recharts 响应式布局)
const ChartCard = ({ title, icon: Icon, children, className = "", action }) => (
  <div className={`bg-slate-900/60 backdrop-blur-md rounded-2xl border border-slate-800 p-5 flex flex-col hover:border-slate-700 transition-colors duration-300 ${className}`}>
    <div className="flex items-center justify-between mb-4 z-10 shrink-0">
      <h3 className="text-sm font-bold text-slate-200 flex items-center gap-2">
        <div className="p-1.5 bg-slate-800 rounded-lg text-cyan-400"><Icon size={16} /></div>
        {title}
      </h3>
      {action}
    </div>
    <div className="flex-1 w-full relative min-h-[200px]">
      <div className="absolute inset-0 flex flex-col">
        {children}
      </div>
    </div>
  </div>
);

// 4.3 专用热力图组件
const ActivityHeatmap = ({ data }) => {
  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  
  const getIntensity = (c) => {
    if (c === 0) return 'bg-slate-800/50';
    if (c < 3) return 'bg-cyan-900/60';
    if (c < 6) return 'bg-cyan-700/70';
    if (c < 12) return 'bg-cyan-600/80';
    if (c < 20) return 'bg-cyan-500/90';
    return 'bg-cyan-400 shadow-[0_0_8px_rgba(34,211,238,0.6)]';
  };

  return (
    <div className="flex flex-col h-full w-full select-none font-mono">
      <div className="flex justify-between text-[10px] text-slate-500 mb-2 px-8">
        {[0, 6, 12, 18, 23].map(h => <span key={h}>{h}H</span>)}
      </div>
      <div className="flex-1 flex flex-col justify-between">
        {days.map((day, dIdx) => (
          <div key={day} className="flex items-center gap-2 group h-full">
            <span className="w-8 text-[10px] text-slate-500 text-right group-hover:text-cyan-400 transition-colors">{day}</span>
            <div className="flex-1 grid gap-1 h-full" style={{ gridTemplateColumns: 'repeat(24, minmax(0, 1fr))' }}>
              {Array.from({ length: 24 }).map((_, hIdx) => (
                <div 
                  key={hIdx} 
                  className={`rounded-sm transition-all duration-300 hover:scale-125 hover:z-10 cursor-crosshair h-full ${getIntensity(data[dIdx]?.[hIdx] || 0)}`} 
                  title={`${day} ${hIdx}:00 - ${data[dIdx]?.[hIdx]} commits`}
                />
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

/* =============================================================================
   MODULE 5: 主应用 (Main Application)
   作用：组合所有模块，管理全局状态（项目、时间范围、筛选器），处理 API 请求。
   ============================================================================= */

export default function App() {
  // --- 状态管理 ---
  const [project, setProject] = useState('Project-A');
  const [range, setRange] = useState('3M');
  const [filter, setFilter] = useState(null);
  const [loading, setLoading] = useState(true);
  const [insight, setInsight] = useState({ text: "", loading: false });

  // --- 数据钩子调用 ---
  const data = useDataProcessor(project, range, filter);

  // --- 副作用处理 (加载动画模拟) ---
  useEffect(() => {
    setLoading(true); 
    setFilter(null); 
    setInsight({ text: "", loading: false });
    const t = setTimeout(() => setLoading(false), 600);
    return () => clearTimeout(t);
  }, [project, range]);

  // --- 交互逻辑: AI 生成洞察 ---
  const generateInsight = async () => {
    if (!APP_CONFIG.apiKey) return setInsight({ text: "请在代码中配置 API Key 以启用 AI 分析功能。", loading: false });
    setInsight({ text: "", loading: true });
    try {
      const res = await fetch(`${APP_CONFIG.apiUrl}?key=${APP_CONFIG.apiKey}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: `作为数据分析师，简要分析开源项目 ${project}：综合得分${data.globalScore.toFixed(2)}, 时区覆盖${data.hriCoverage}%, 活跃地区${data.geoData.countryData.length}个。给出3个关键洞察。` }] }],
        })
      });
      const resJson = await res.json();
      setInsight({ text: resJson.candidates?.[0]?.content?.parts?.[0]?.text || "分析服务暂时不可用", loading: false });
    } catch (e) {
      setInsight({ text: "网络连接异常", loading: false });
    }
  };

  // --- 交互逻辑: 导出数据 ---
  const exportJson = () => {
    const blob = new Blob([JSON.stringify(data.rawData, null, 2)], { type: 'application/json' });
    const a = Object.assign(document.createElement('a'), { href: URL.createObjectURL(blob), download: `${project}_${range}.json` });
    a.click();
  };

  // --- 渲染: 加载态 ---
  if (loading) {
    return (
      <div className="min-h-screen bg-slate-950 flex flex-col items-center justify-center gap-4">
        <div className="w-12 h-12 border-4 border-cyan-500/30 border-t-cyan-500 rounded-full animate-spin"></div>
        <p className="text-cyan-500 font-mono text-sm tracking-widest animate-pulse">SYSTEM INITIALIZING...</p>
      </div>
    );
  }

  // --- 渲染: 主界面 ---
  return (
    <div className="min-h-screen bg-slate-950 text-slate-200 font-sans selection:bg-cyan-500/30">
      
      {/* 顶部导航栏 */}
      <nav className="h-16 border-b border-slate-800 bg-slate-950/80 backdrop-blur-xl sticky top-0 z-50 px-6 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <div className="bg-cyan-500 w-8 h-8 rounded flex items-center justify-center shadow-[0_0_15px_rgba(6,182,212,0.5)]">
            <LayoutDashboard size={18} className="text-white" />
          </div>
          <h1 className="text-lg font-bold tracking-tight text-white hidden sm:block">
            Global<span className="text-cyan-400">Pulse</span> <span className="text-slate-500 font-light">| Analytics</span>
          </h1>
        </div>
        <div className="flex items-center gap-3">
          <div className="hidden md:flex bg-slate-900 p-1 rounded-lg border border-slate-800">
            {TIME_RANGES.map(r => (
              <button 
                key={r.value} 
                onClick={() => setRange(r.value)} 
                className={`px-4 py-1.5 text-xs font-medium rounded-md transition-all ${range === r.value ? 'bg-slate-800 text-cyan-400 shadow-sm' : 'text-slate-500 hover:text-slate-300'}`}
              >
                {r.label}
              </button>
            ))}
          </div>
          <div className="h-6 w-px bg-slate-800 mx-2 hidden md:block"></div>
          <select 
            value={project} 
            onChange={e => setProject(e.target.value)} 
            className="bg-slate-900 border border-slate-800 text-sm py-1.5 px-3 rounded-lg outline-none focus:border-cyan-500/50 text-slate-300 font-medium"
          >
            <option value="Project-A">React Native</option>
            <option value="Project-B">TensorFlow</option>
            <option value="Project-C">Kubernetes</option>
          </select>
          <button onClick={exportJson} className="p-2 hover:bg-slate-800 text-slate-400 hover:text-cyan-400 rounded-lg transition-colors">
            <Download size={18} />
          </button>
        </div>
      </nav>

      {/* 主仪表盘布局 */}
      <main className="p-4 md:p-6 max-w-[2400px] mx-auto space-y-4">
        
        {/* KPI 关键指标区 */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <MetricCard title="Global Score" value={data.globalScore.toFixed(2)} unit="/ 1.0" icon={Activity} subValue="Health Index" change={12} color="text-cyan-400" />
          <MetricCard title="Contributors" value={data.totalContributors} icon={Users} subValue="Unique Devs" change={5} color="text-violet-400" />
          <MetricCard title="Timezone Coverage" value={data.hriCoverage} unit="%" icon={Globe} subValue="24H Active" change={-2} color="text-emerald-400" />
          <MetricCard title="Diversity Score" value={data.geoData.diversityScore.toFixed(2)} icon={MapIcon} subValue={`${data.geoData.countryData.length} Regions`} change={8} color="text-rose-400" />
        </div>

        {/* 核心可视化网格 */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 auto-rows-[minmax(300px,auto)]">
          
          {/* Chart 1: 面积趋势图 */}
          <ChartCard title="Contribution Velocity & Trends" icon={TrendingUp} className="lg:col-span-8 lg:row-span-2 h-[400px]">
            <ResponsiveContainer width="100%" height="100%" minWidth={0} minHeight={0} debounce={200}>
              <AreaChart data={data.history} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="colorScore" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#06b6d4" stopOpacity={0}/>
                  </linearGradient>
                  <linearGradient id="colorCommits" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid {...THEME.chart.grid} vertical={false} />
                <XAxis dataKey="label" {...THEME.chart.axis} dy={10} />
                <YAxis yAxisId="left" {...THEME.chart.axis} domain={[0, 1]} />
                <YAxis yAxisId="right" orientation="right" {...THEME.chart.axis} />
                <Tooltip {...THEME.chart.tooltip} />
                <Area yAxisId="left" type="monotone" dataKey="score" stroke="#06b6d4" strokeWidth={3} fillOpacity={1} fill="url(#colorScore)" name="Health Score" />
                <Area yAxisId="right" type="monotone" dataKey="commits" stroke="#8b5cf6" strokeWidth={3} fillOpacity={1} fill="url(#colorCommits)" name="Commits" />
              </AreaChart>
            </ResponsiveContainer>
          </ChartCard>

          {/* Chart 2: 贡献者列表 (带筛选) */}
          <ChartCard 
            title="Top Contributors" icon={Award} className="lg:col-span-4 lg:row-span-2 overflow-hidden h-[400px] lg:h-auto"
            action={filter && (
              <button onClick={() => setFilter(null)} className="text-[10px] bg-rose-500/20 text-rose-400 px-2 py-1 rounded border border-rose-500/30 hover:bg-rose-500/30 flex items-center gap-1">
                <XCircle size={10}/>Clear {filter}
              </button>
            )}
          >
            <div className="flex-1 overflow-y-auto pr-2 custom-scrollbar -mr-2">
              <table className="w-full text-left text-xs border-collapse">
                <thead className="sticky top-0 bg-slate-900/95 backdrop-blur z-10 text-slate-500 font-medium">
                  <tr>
                    <th className="py-3 pl-2">Rank</th>
                    <th className="py-3">Developer</th>
                    <th className="py-3 text-right">Region</th>
                    <th className="py-3 text-right pr-2">Commits</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/50">
                  {data.filteredContributors.map((u, i) => (
                    <tr key={u.contributor_id} className="group hover:bg-slate-800/40 transition-colors">
                      <td className="py-2.5 pl-2 font-mono text-slate-600 group-hover:text-cyan-400 transition-colors">
                        {String(i+1).padStart(2,'0')}
                      </td>
                      <td className="py-2.5">
                        <div className="flex items-center gap-2">
                          <div className={`w-6 h-6 rounded-md flex items-center justify-center text-[10px] font-bold ${i < 3 ? 'bg-gradient-to-br from-yellow-400 to-orange-500 text-black' : 'bg-slate-800 text-slate-400'}`}>
                            {u.contributor_name[0]}
                          </div>
                          <span className="text-slate-300 font-medium">{u.contributor_name}</span>
                        </div>
                      </td>
                      <td className="py-2.5 text-right">
                        <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-mono border ${u.location_iso3 === filter ? 'bg-cyan-500/20 border-cyan-500/50 text-cyan-300' : 'bg-slate-800 border-slate-700 text-slate-500'}`}>
                          {u.location_iso3}
                        </span>
                      </td>
                      <td className="py-2.5 text-right pr-2 font-mono text-slate-200">{u.count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </ChartCard>

          {/* Chart 3: 饼图分布 (筛选器触发源) */}
          <ChartCard title="Geo Distribution" icon={Globe} className="lg:col-span-3 h-[280px]">
            <ResponsiveContainer width="100%" height="100%" minWidth={0} minHeight={0} debounce={200}>
              <PieChart>
                <Pie 
                  data={data.geoData.pieData} innerRadius="60%" outerRadius="80%" paddingAngle={5} dataKey="value" stroke="none"
                  onClick={d => d.name !== 'Other' && setFilter(d.name === filter ? null : d.name)}
                  className="cursor-pointer outline-none"
                >
                  {data.geoData.pieData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={THEME.colors[index % THEME.colors.length]} opacity={filter && filter !== entry.name ? 0.3 : 1} />
                  ))}
                </Pie>
                <Tooltip {...THEME.chart.tooltip} />
                <Legend verticalAlign="bottom" iconSize={8} wrapperStyle={{fontSize:'10px', paddingTop:'10px'}} />
              </PieChart>
            </ResponsiveContainer>
          </ChartCard>

          {/* Chart 4: 雷达能力图 */}
          <ChartCard title="Health Radar" icon={Target} className="lg:col-span-3 h-[280px]">
            <ResponsiveContainer width="100%" height="100%" minWidth={0} minHeight={0} debounce={200}>
              <RadarChart cx="50%" cy="50%" outerRadius="70%" data={data.radarData}>
                <PolarGrid stroke="#334155" />
                <PolarAngleAxis dataKey="subject" tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <PolarRadiusAxis angle={30} domain={[0, 150]} tick={false} axisLine={false} />
                <Radar name="Current" dataKey="A" stroke="#06b6d4" strokeWidth={2} fill="#06b6d4" fillOpacity={0.4} />
                <Tooltip {...THEME.chart.tooltip} />
              </RadarChart>
            </ResponsiveContainer>
          </ChartCard>

          {/* Chart 5: 热力图 */}
          <ChartCard title="Weekly Rhythm" icon={Calendar} className="lg:col-span-6 h-[280px]">
            <ActivityHeatmap data={data.heatmapData} />
          </ChartCard>

          {/* Feature: AI 分析面板 */}
          <div className="lg:col-span-12 relative group">
            <div className="absolute inset-0 bg-gradient-to-r from-cyan-600/20 to-violet-600/20 rounded-2xl blur-xl opacity-50 group-hover:opacity-100 transition-opacity"></div>
            <div className="relative bg-slate-900/80 backdrop-blur-xl border border-slate-700/50 rounded-2xl p-6 flex flex-col md:flex-row gap-6 items-start">
              <div className="md:w-1/4 space-y-4">
                <div className="flex items-center gap-2 text-cyan-400">
                  <Sparkles size={20} className="animate-pulse" />
                  <h3 className="text-lg font-bold text-white">AI Diagnostics</h3>
                </div>
                <p className="text-xs text-slate-400 leading-relaxed">
                  Generate instant insights based on current metrics using Gemini 2.5 Flash model.
                </p>
                <button 
                  onClick={generateInsight} 
                  disabled={insight.loading}
                  className="w-full py-2.5 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white text-sm font-bold rounded-lg shadow-lg shadow-cyan-900/20 transition-all flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {insight.loading ? <div className="w-4 h-4 border-2 border-white/30 border-t-white animate-spin rounded-full"/> : <Zap size={16} fill="currentColor" />}
                  Generate Report
                </button>
              </div>
              
              <div className="md:w-3/4 w-full bg-slate-950/50 rounded-xl border border-slate-800 p-5 min-h-[120px] flex items-center">
                {insight.text ? (
                  <div className="prose prose-invert prose-sm max-w-none">
                    <p className="text-slate-300 leading-relaxed whitespace-pre-wrap">{insight.text}</p>
                  </div>
                ) : (
                  <div className="w-full flex flex-col items-center justify-center text-slate-600 gap-2">
                    <Activity size={24} className="opacity-20" />
                    <p className="text-sm">Ready to analyze {data.totalCommits} data points...</p>
                  </div>
                )}
              </div>
            </div>
          </div>

        </div>
      </main>
      
      <footer className="py-6 text-center text-slate-600 text-xs font-mono">
        GLOBAL PULSE ANALYTICS © 2025 • POWERED BY REACT & RECHARTS
      </footer>
    </div>
  );
}
