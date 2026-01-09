import React, { useState, useMemo, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend, LineChart, Line, AreaChart, Area,
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis
} from 'recharts';
import { 
  Globe, Clock, TrendingUp, HelpCircle, Sparkles, Activity, 
  Calendar, Map as MapIcon, Award, Users, Target, Clock4, 
  Filter, Download, XCircle, Zap, LayoutDashboard
} from 'lucide-react';

// --- 全局配置与常量定义 ---
const API_KEY = ""; // 注意：此处需要填写您的 Google Gemini API Key
const API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent";

// 时间范围选项配置
const TIME_RANGES = [
  { label: '30D', value: '1M', days: 30 },
  { label: '90D', value: '3M', days: 90 },
  { label: '180D', value: '6M', days: 180 },
  { label: '1Y', value: '1Y', days: 365 },
];

// 视觉样式配置 (霓虹配色与图表主题)
const COLORS = ['#06b6d4', '#8b5cf6', '#f43f5e', '#f59e0b', '#10b981', '#3b82f6'];
const CHART_THEME = {
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
};

// --- 核心逻辑：模拟数据生成器 ---
// 根据项目类型(isA)和天数生成差异化的提交记录、历史趋势和雷达图数据
const generateMockData = (projectType, days) => {
  const now = Math.floor(Date.now() / 1000);
  const isA = projectType === 'Project-A'; // 区分不同项目的特征
  const count = isA ? days * 12 : days * 6; // Project-A 数据更密集
  
  // 1. 生成提交记录 (Commits)
  const commits = Array.from({ length: count }, () => {
    const ts = now - Math.floor(Math.random() * 86400 * days);
    const hour = new Date(ts * 1000).getUTCHours();
    // 基础地区池
    let pool = isA ? ["USA", "CHN", "DEU", "IND", "JPN"] : ["USA", "GBR", "DEU", "CAN"];
    
    // 模拟真实时区特征：根据生成的小时数调整地区概率
    if (isA) {
      if (hour >= 2 && hour <= 10) pool = ["CHN", "IND", "JPN", "AUS"]; // 亚洲时段
      else if (hour >= 10 && hour <= 18) pool = ["DEU", "GBR", "FRA", "NLD"]; // 欧洲时段
    }
    
    return {
      timestamp_unix: ts,
      location_iso3: pool[Math.floor(Math.random() * pool.length)],
      contributor_id: `u${Math.floor(Math.random() * (isA ? 80 : 30))}`, // 模拟不同规模的贡献者池
      contributor_name: `Dev-${Math.floor(Math.random() * 9000) + 1000}`
    };
  }).sort((a, b) => a.timestamp_unix - b.timestamp_unix);

  // 2. 生成历史趋势数据 (History)
  const historyPoints = days > 90 ? 20 : 10;
  const history = Array.from({ length: historyPoints }, (_, i) => ({
    label: days > 180 ? `${i+1}M` : `W${i+1}`,
    score: 0.5 + Math.random() * 0.4, // 随机健康分
    commits: Math.floor(Math.random() * 150) + 40,
    contributors: Math.floor(Math.random() * 50) + 10
  }));

  // 3. 生成雷达图能力数据 (Radar)
  const radarData = [
    { subject: 'Timezone', A: isA ? 130 : 60, full: 150 },
    { subject: 'Diversity', A: isA ? 120 : 40, full: 150 },
    { subject: 'Consistency', A: isA ? 110 : 90, full: 150 },
    { subject: 'Activity', A: isA ? 140 : 80, full: 150 },
    { subject: 'Retention', A: isA ? 95 : 60, full: 150 },
    { subject: 'Language', A: isA ? 100 : 20, full: 150 },
  ];

  return { commits, history, radarData };
};

// --- 自定义 Hook：数据处理核心 ---
// 负责将原始数据转换为图表所需的各种格式，并利用 useMemo 进行性能优化
const useDataProcessor = (project, range, filter) => {
  // 第一层 Memo: 仅当项目或时间范围改变时重新生成"原始数据"
  const rawData = useMemo(() => {
    const config = TIME_RANGES.find(r => r.value === range) || TIME_RANGES[0];
    return generateMockData(project, config.days);
  }, [project, range]);

  // 第二层 Memo: 基于原始数据进行聚合计算 (过滤、统计等)
  return useMemo(() => {
    const { commits, history, radarData } = rawData;
    
    // 初始化计数器
    const hourly = Array(24).fill(0); 
    const heatmap = Array(7).fill(0).map(() => Array(24).fill(0)); // 7天 * 24小时矩阵
    const geo = {}; 
    const users = {};

    // 单次遍历所有 commits 进行数据聚合 (O(N) 复杂度)
    commits.forEach(c => {
      const d = new Date(c.timestamp_unix * 1000);
      const h = d.getUTCHours(), day = d.getUTCDay();
      
      // 填充热力图与小时统计
      hourly[h]++; 
      heatmap[day][h]++;
      
      // 统计地理分布
      geo[c.location_iso3] = (geo[c.location_iso3] || 0) + 1;
      
      // 统计用户贡献
      if (!users[c.contributor_id]) users[c.contributor_id] = { ...c, count: 0 };
      users[c.contributor_id].count++;
    });

    // 数据格式化适配 Recharts
    const hriData = hourly.map((c, h) => ({ hour: h.toString().padStart(2, '0'), commits: c }));
    const countryData = Object.entries(geo).sort((a, b) => b[1] - a[1]).map(([name, value]) => ({ name, value }));
    
    // 饼图数据优化：只显示前5名，其余归为 "Other"
    const pieData = [...countryData.slice(0, 5), ...(countryData.length > 5 ? [{ name: 'Other', value: countryData.slice(5).reduce((s, x) => s + x.value, 0) }] : [])];
    
    // 处理用户过滤逻辑
    let filteredUsers = Object.values(users);
    if (filter) filteredUsers = filteredUsers.filter(u => u.location_iso3 === filter);
    
    // 计算综合得分算法
    const hriScore = hourly.filter(v => v > 0).length / 24; // 活跃时段占比
    const geoScore = countryData.length > 0 ? Math.min(1, Math.log(countryData.length) / Math.log(8)) : 0; // 地理多样性对数得分

    return {
      hriData, 
      heatmapData: heatmap, 
      radarData, 
      history,
      geoData: { countryData, pieData, diversityScore: geoScore },
      totalCommits: commits.length,
      totalContributors: Object.keys(users).length,
      globalScore: (hriScore + geoScore) / 2, // 简单平均分
      hriCoverage: Math.round(hriScore * 100),
      filteredContributors: filteredUsers.sort((a, b) => b.count - a.count).slice(0, 20), // Top 20 贡献者
      rawData // 保留原始数据用于导出
    };
  }, [rawData, filter]); // 依赖项：原始数据变化 或 过滤器变化时重新计算
};

// --- 子组件：活动热力图 ---
// 渲染类似 GitHub Contribution 的 7x24 网格
const ActivityHeatmap = ({ data }) => {
  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  
  // 根据 commit 数量返回对应的 Tailwind 颜色类名
  const getIntensity = (c) => {
    if (c === 0) return 'bg-slate-800/50';
    if (c < 3) return 'bg-cyan-900/60';
    if (c < 6) return 'bg-cyan-700/70';
    if (c < 9) return 'bg-cyan-600/80';
    if (c < 12) return 'bg-cyan-500/90';
    return 'bg-cyan-400 shadow-[0_0_8px_rgba(34,211,238,0.6)]'; // 高亮样式
  };

  return (
    <div className="flex flex-col h-full w-full select-none font-mono">
      {/* 顶部小时轴 */}
      <div className="flex justify-between text-[10px] text-slate-500 mb-2 px-8">
        {[0, 6, 12, 18, 23].map(h => <span key={h}>{h}H</span>)}
      </div>
      {/* 热力图主体 */}
      <div className="flex-1 flex flex-col justify-between">
        {days.map((day, dIdx) => (
          <div key={day} className="flex items-center gap-2 group">
            <span className="w-8 text-[10px] text-slate-500 text-right group-hover:text-cyan-400 transition-colors">{day}</span>
            <div className="flex-1 grid grid-cols-24 gap-1 h-full">
              {Array.from({ length: 24 }).map((_, hIdx) => (
                <div 
                  key={hIdx} 
                  className={`rounded-sm transition-all duration-300 hover:scale-125 hover:z-10 cursor-crosshair ${getIntensity(data[dIdx]?.[hIdx] || 0)}`} 
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

// --- 通用 UI 组件 ---
// 指标卡片 (Key Metrics)
const MetricCard = ({ title, value, icon: Icon, unit, subValue, change, color }) => (
  <div className="relative overflow-hidden bg-slate-900/60 backdrop-blur-md p-5 rounded-2xl border border-slate-800 group hover:border-slate-700 transition-all duration-300 hover:shadow-lg hover:shadow-cyan-900/10">
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

// 图表容器卡片 (通用外壳)
const ChartCard = ({ title, icon: Icon, children, className = "", action }) => (
  <div className={`bg-slate-900/60 backdrop-blur-md rounded-2xl border border-slate-800 p-5 flex flex-col hover:border-slate-700 transition-colors duration-300 relative overflow-hidden ${className}`}>
    <div className="flex items-center justify-between mb-4 z-10">
      <h3 className="text-sm font-bold text-slate-200 flex items-center gap-2">
        <div className="p-1.5 bg-slate-800 rounded-lg text-cyan-400"><Icon size={16} /></div>
        {title}
      </h3>
      {action}
    </div>
    <div className="flex-1 w-full min-h-0 relative z-10">
      {children}
    </div>
  </div>
);

// --- 主应用组件 ---
export default function App() {
  // 状态管理
  const [project, setProject] = useState('Project-A');
  const [range, setRange] = useState('3M');
  const [filter, setFilter] = useState(null); // 用于过滤特定的国家/地区
  const [loading, setLoading] = useState(true);
  const [insight, setInsight] = useState({ text: "", loading: false }); // AI 分析结果状态

  // 获取处理后的数据
  const data = useDataProcessor(project, range, filter);

  // 模拟加载效果 (切换项目或时间时触发)
  useEffect(() => {
    setLoading(true); setFilter(null); setInsight({ text: "", loading: false });
    const t = setTimeout(() => setLoading(false), 600);
    return () => clearTimeout(t);
  }, [project, range]);

  // 调用 AI 接口生成分析报告
  const generateInsight = async () => {
    if (!API_KEY) return setInsight({ text: "请配置 API Key 以启用 AI 分析功能。", loading: false });
    setInsight({ text: "", loading: true });
    try {
      const res = await fetch(`${API_URL}?key=${API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          // 构建 Prompt：将当前的统计数据发送给 AI
          contents: [{ parts: [{ text: `作为数据分析师，简要分析开源项目 ${project}：综合得分${data.globalScore.toFixed(2)}, 时区覆盖${data.hriCoverage}%, 活跃地区${data.geoData.countryData.length}个。给出3个关键洞察。` }] }],
        })
      });
      const resJson = await res.json();
      setInsight({ text: resJson.candidates?.[0]?.content?.parts?.[0]?.text || "分析服务暂时不可用", loading: false });
    } catch (e) {
      setInsight({ text: "网络连接异常", loading: false });
    }
  };

  // 导出 JSON 功能
  const exportJson = () => {
    const blob = new Blob([JSON.stringify(data.rawData, null, 2)], { type: 'application/json' });
    const a = Object.assign(document.createElement('a'), { href: URL.createObjectURL(blob), download: `${project}_${range}.json` });
    a.click();
  };

  // Loading 界面
  if (loading && !data) return (
    <div className="min-h-screen bg-slate-950 flex flex-col items-center justify-center gap-4">
      <div className="w-12 h-12 border-4 border-cyan-500/30 border-t-cyan-500 rounded-full animate-spin"></div>
      <p className="text-cyan-500 font-mono text-sm tracking-widest animate-pulse">SYSTEM INITIALIZING...</p>
    </div>
  );

  return (
    <div className="min-h-screen bg-slate-950 text-slate-200 font-sans selection:bg-cyan-500/30">
      
      {/* --- 顶部导航栏 --- */}
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
           {/* 时间范围选择器 */}
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
          {/* 项目下拉框 */}
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

      {/* --- 主内容区：Bento Grid 网格布局 --- */}
      <main className="p-4 md:p-6 max-w-[2400px] mx-auto space-y-4">
        
        {/* Row 1: 关键指标卡片区 (Grid 4) */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <MetricCard title="Global Score" value={data.globalScore.toFixed(2)} unit="/ 1.0" icon={Activity} subValue="Health Index" change={12} color="text-cyan-400" />
          <MetricCard title="Contributors" value={data.totalContributors} icon={Users} subValue="Unique Devs" change={5} color="text-violet-400" />
          <MetricCard title="Timezone Coverage" value={data.hriCoverage} unit="%" icon={Globe} subValue="24H Active" change={-2} color="text-emerald-400" />
          <MetricCard title="Diversity Score" value={data.geoData.diversityScore.toFixed(2)} icon={MapIcon} subValue={`${data.geoData.countryData.length} Regions`} change={8} color="text-rose-400" />
        </div>

        {/* Row 2: 核心图表混合区 (采用 Grid 12 进行精细布局) */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 auto-rows-[minmax(300px,auto)]">
          
          {/* Main Chart: 历史趋势图 (占据 8/12 列宽，2行高) */}
          <ChartCard title="Contribution Velocity & Trends" icon={TrendingUp} className="lg:col-span-8 lg:row-span-2 min-h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={data.history} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <defs>
                  {/* 定义渐变填充色 */}
                  <linearGradient id="colorScore" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#06b6d4" stopOpacity={0}/>
                  </linearGradient>
                  <linearGradient id="colorCommits" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid {...CHART_THEME.grid} vertical={false} />
                <XAxis dataKey="label" {...CHART_THEME.axis} dy={10} />
                {/* 双 Y 轴配置 */}
                <YAxis yAxisId="left" {...CHART_THEME.axis} domain={[0, 1]} />
                <YAxis yAxisId="right" orientation="right" {...CHART_THEME.axis} />
                <Tooltip {...CHART_THEME.tooltip} />
                <Area yAxisId="left" type="monotone" dataKey="score" stroke="#06b6d4" strokeWidth={3} fillOpacity={1} fill="url(#colorScore)" name="Health Score" />
                <Area yAxisId="right" type="monotone" dataKey="commits" stroke="#8b5cf6" strokeWidth={3} fillOpacity={1} fill="url(#colorCommits)" name="Commits" />
              </AreaChart>
            </ResponsiveContainer>
          </ChartCard>

          {/* Right Column: 贡献者列表 (占据 4/12 列宽，2行高) */}
          <ChartCard 
            title="Top Contributors" 
            icon={Award} 
            className="lg:col-span-4 lg:row-span-2 overflow-hidden flex flex-col"
            // 显示清除过滤器的按钮
            action={filter && <button onClick={() => setFilter(null)} className="text-[10px] bg-rose-500/20 text-rose-400 px-2 py-1 rounded border border-rose-500/30 hover:bg-rose-500/30 flex items-center gap-1"><XCircle size={10}/>Clear {filter}</button>}
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
                      <td className="py-2.5 pl-2 font-mono text-slate-600 group-hover:text-cyan-400 transition-colors">{String(i+1).padStart(2,'0')}</td>
                      <td className="py-2.5">
                        <div className="flex items-center gap-2">
                          {/* 头像渲染：前三名使用特殊渐变色 */}
                          <div className={`w-6 h-6 rounded-md flex items-center justify-center text-[10px] font-bold ${i < 3 ? 'bg-gradient-to-br from-yellow-400 to-orange-500 text-black' : 'bg-slate-800 text-slate-400'}`}>
                            {u.contributor_name[0]}
                          </div>
                          <span className="text-slate-300 font-medium">{u.contributor_name}</span>
                        </div>
                      </td>
                      <td className="py-2.5 text-right">
                        {/* 地区标签：高亮当前选中过滤器 */}
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

          {/* Row 3 Components: 底部小图表 */}
          
          {/* Pie Chart: 饼图 (3/12 列宽) */}
          <ChartCard title="Geo Distribution" icon={Globe} className="lg:col-span-3 min-h-[280px]">
            <ResponsiveContainer>
              <PieChart>
                <Pie 
                  data={data.geoData.pieData} 
                  innerRadius="60%" 
                  outerRadius="80%" 
                  paddingAngle={5} 
                  dataKey="value"
                  stroke="none"
                  // 交互：点击扇区进行过滤
                  onClick={d => d.name !== 'Other' && setFilter(d.name === filter ? null : d.name)}
                  className="cursor-pointer outline-none"
                >
                  {data.geoData.pieData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} opacity={filter && filter !== entry.name ? 0.3 : 1} />
                  ))}
                </Pie>
                <Tooltip {...CHART_THEME.tooltip} />
                <Legend verticalAlign="bottom" iconSize={8} wrapperStyle={{fontSize:'10px', paddingTop:'10px'}} />
              </PieChart>
            </ResponsiveContainer>
          </ChartCard>

          {/* Radar Chart: 雷达图 (3/12 列宽) */}
          <ChartCard title="Health Radar" icon={Target} className="lg:col-span-3 min-h-[280px]">
             <ResponsiveContainer>
              <RadarChart cx="50%" cy="50%" outerRadius="70%" data={data.radarData}>
                <PolarGrid stroke="#334155" />
                <PolarAngleAxis dataKey="subject" tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <PolarRadiusAxis angle={30} domain={[0, 150]} tick={false} axisLine={false} />
                <Radar name="Current" dataKey="A" stroke="#06b6d4" strokeWidth={2} fill="#06b6d4" fillOpacity={0.4} />
                <Tooltip {...CHART_THEME.tooltip} />
              </RadarChart>
            </ResponsiveContainer>
          </ChartCard>

          {/* Heatmap: 热力图 (6/12 列宽) */}
          <ChartCard title="Weekly Rhythm" icon={Calendar} className="lg:col-span-6 min-h-[280px]">
            <ActivityHeatmap data={data.heatmapData} />
          </ChartCard>

          {/* Bottom Full: AI 分析板块 (12/12 列宽) */}
          <div className="lg:col-span-12 relative group">
            {/* 装饰性背景光晕 */}
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
              
              {/* AI 分析结果展示区域 */}
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
      
      {/* 底部版权 */}
      <footer className="py-6 text-center text-slate-600 text-xs font-mono">
        GLOBAL PULSE ANALYTICS © 2025 • POWERED BY REACT & RECHARTS
      </footer>
    </div>
  );
}
