import React, { useState, useMemo, useEffect, useCallback, useRef } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend, LineChart, Line, AreaChart, Area,
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis
} from 'recharts';
import { 
  Globe, Clock, TrendingUp, HelpCircle, Sparkles, Activity, 
  Calendar, Map as MapIcon, Award, Users, Target, Clock4, 
  Filter, Download, RefreshCw, XCircle, ChevronDown 
} from 'lucide-react';

// --- GEMINI API 配置 ---
const API_KEY = ""; // 请替换为您的 Gemini API 密钥
const API_URL_GEMINI = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent";

// --- 常量定义 ---
const TIME_RANGES = [
  { label: '近30天', value: '1M', days: 30 },
  { label: '近90天', value: '3M', days: 90 },
  { label: '近半年', value: '6M', days: 180 },
  { label: '近一年', value: '1Y', days: 365 },
];

const COLORS = ['#06b6d4', '#f59e0b', '#ec4899', '#8b5cf6', '#10b981', '#ef4444', '#f97316', '#a1a1aa'];
const RADAR_COLOR = '#06b6d4';

// --- DYNAMIC DATA GENERATOR (动态数据生成引擎) ---
// 改进：根据时间范围动态生成数据，模拟真实场景
const generateMockData = (projectType, days) => {
  const now = Math.floor(Date.now() / 1000);
  const commitsCount = projectType === 'Project-A' ? Math.floor(days * 8) : Math.floor(days * 4);
  
  // 1. 生成提交记录
  const commits = Array(commitsCount).fill(null).map(() => {
    const randomOffset = Math.floor(Math.random() * 86400 * days);
    const timestamp = now - randomOffset;
    const hour = new Date(timestamp * 1000).getUTCHours();
    
    // 模拟 Project A 更加国际化，Project B 偏向欧美
    let locationPool;
    if (projectType === 'Project-A') {
        // 根据时间稍微偏移地区，模拟时区规律
        locationPool = ["USA", "CHN", "DEU", "IND", "JPN", "GBR", "BRA", "AUS", "CAN", "FRA"];
        // 简单的时区权重模拟
        if (hour >= 1 && hour <= 9) locationPool = ["CHN", "IND", "JPN", "AUS"]; 
        else if (hour >= 9 && hour <= 17) locationPool = ["DEU", "GBR", "FRA", "USA"];
        else locationPool = ["USA", "CAN", "BRA"];
    } else {
        locationPool = ["USA", "GBR", "DEU", "CAN"];
    }
    
    const location = locationPool[Math.floor(Math.random() * locationPool.length)];

    return {
      timestamp_unix: timestamp,
      location_iso3: location,
      contributor_id: `u${Math.floor(Math.random() * (projectType === 'Project-A' ? 50 : 15))}`,
      contributor_name: `Dev-${Math.floor(Math.random() * 1000)}`
    };
  }).sort((a, b) => a.timestamp_unix - b.timestamp_unix);

  // 2. 生成趋势历史 (按月/周聚合)
  const historyPoints = days > 90 ? 12 : 6; // 时间长则点多
  const history = Array(historyPoints).fill(null).map((_, i) => {
    return {
        label: days > 180 ? `${i+1}月` : `W${i+1}`,
        score: 0.4 + Math.random() * 0.4,
        commits: Math.floor(Math.random() * 100) + 20
    };
  });

  // 3. 生成区域堆叠数据
  const regionalHistory = history.map(h => ({
      label: h.label,
      'North America': Math.floor(Math.random() * 50),
      'Asia': projectType === 'Project-A' ? Math.floor(Math.random() * 60) : Math.floor(Math.random() * 10),
      'Europe': Math.floor(Math.random() * 40),
      'Others': Math.floor(Math.random() * 20),
  }));

  // 4. 雷达图数据 (Project A 优于 Project B)
  const isA = projectType === 'Project-A';
  const radarData = [
    { subject: '时区覆盖', A: isA ? 130 : 60, fullMark: 150 },
    { subject: '地域多元性', A: isA ? 120 : 40, fullMark: 150 },
    { subject: '贡献连续性', A: isA ? 110 : 90, fullMark: 150 },
    { subject: '社区活跃度', A: isA ? 140 : 80, fullMark: 150 },
    { subject: '新人留存', A: isA ? 95 : 60, fullMark: 150 },
    { subject: '非英语母语', A: isA ? 100 : 20, fullMark: 150 },
  ];

  return { commits, history, regionalHistory, radarData };
};


// --- 辅助组件 ---
const ActivityHeatmap = ({ data }) => {
    const days = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];
    const hours = Array.from({ length: 24 }, (_, i) => i);
    const getIntensity = (count) => {
        if (count === 0) return 'bg-gray-800/50';
        if (count < 2) return 'bg-cyan-900/40';
        if (count < 5) return 'bg-cyan-700/60';
        if (count < 8) return 'bg-cyan-500/80';
        return 'bg-cyan-400';
    };

    return (
        <div className="flex flex-col h-full w-full overflow-x-auto select-none">
             <div className="flex mb-2">
                <div className="w-8"></div>
                <div className="flex-1 flex justify-between text-[10px] text-gray-500 px-1 font-mono">
                    {hours.filter(h => h % 3 === 0).map(h => <span key={h}>{h}H</span>)}
                </div>
             </div>
            {days.map((day, dayIdx) => (
                <div key={day} className="flex items-center mb-1 group">
                    <span className="w-8 text-[10px] text-gray-500 text-right pr-2 group-hover:text-cyan-400 transition-colors">{day}</span>
                    <div className="flex-1 grid grid-cols-24 gap-[2px]">
                        {hours.map(hour => {
                            const count = data[dayIdx]?.[hour] || 0;
                            return (
                                <div 
                                    key={`${dayIdx}-${hour}`}
                                    className={`h-5 rounded-[2px] transition-all hover:scale-110 hover:shadow-lg hover:z-10 cursor-crosshair ${getIntensity(count)}`}
                                    title={`${day} ${hour}:00 UTC - ${count} 次提交`}
                                />
                            );
                        })}
                    </div>
                </div>
            ))}
        </div>
    );
};

// --- 核心数据处理 HOOK ---
const useDataProcessor = (selectedProject, timeRange, regionFilter) => {
  // 1. 模拟数据获取 (依赖 timeRange 变化)
  const rawData = useMemo(() => {
     const rangeConfig = TIME_RANGES.find(r => r.value === timeRange) || TIME_RANGES[0];
     return generateMockData(selectedProject, rangeConfig.days);
  }, [selectedProject, timeRange]);

  return useMemo(() => {
    const { commits, history, regionalHistory, radarData } = rawData;

    if (!commits.length) return null;

    // --- 数据聚合逻辑 ---
    
    // 1. 初始化容器
    const hourlyCounts = Array(24).fill(0);
    const heatmapGrid = Array(7).fill(null).map(() => Array(24).fill(0));
    const countryCounts = {};
    const contributorStats = {};

    // 2. 遍历 Commits
    commits.forEach(c => {
      const date = new Date(c.timestamp_unix * 1000);
      const utcHour = date.getUTCHours();
      const day = date.getUTCDay();
      
      hourlyCounts[utcHour]++;
      heatmapGrid[day][utcHour]++;

      // Geo
      countryCounts[c.location_iso3] = (countryCounts[c.location_iso3] || 0) + 1;

      // Contributors
      if (!contributorStats[c.contributor_id]) {
          contributorStats[c.contributor_id] = { 
              id: c.contributor_id, 
              name: c.contributor_name,
              count: 0,
              location: c.location_iso3 
          };
      }
      contributorStats[c.contributor_id].count++;
    });

    // 3. 格式化图表数据
    const hriData = hourlyCounts.map((count, hour) => ({
      hour: `${hour.toString().padStart(2, '0')}`,
      commits: count,
    }));

    const countryData = Object.entries(countryCounts)
      .sort(([, a], [, b]) => b - a)
      .map(([name, value]) => ({ name, value }));
      
    // Pie Chart Data
    const top5Countries = countryData.slice(0, 5);
    const otherCommits = countryData.slice(5).reduce((sum, c) => sum + c.value, 0);
    const pieData = [
        ...top5Countries,
        ...(otherCommits > 0 ? [{ name: 'Others', value: otherCommits }] : [])
    ];

    // 4. 贡献者列表 (支持筛选)
    let filteredContributors = Object.values(contributorStats);
    if (regionFilter) {
        filteredContributors = filteredContributors.filter(c => c.location === regionFilter);
    }
    filteredContributors = filteredContributors.sort((a, b) => b.count - a.count).slice(0, 10);

    // 5. 核心指标计算
    const numUniqueCountries = Object.keys(countryCounts).length;
    // 简单的熵计算模拟
    const diversityScore = numUniqueCountries > 0
      ? parseFloat((Math.min(1, Math.log(numUniqueCountries) / Math.log(8))).toFixed(2))
      : 0;
    
    const hriCoverage = hriData.filter(d => d.commits > 0).length / 24;
    const hriScore = parseFloat(hriCoverage.toFixed(2));
    const globalScore = parseFloat(((hriScore * 0.5) + (diversityScore * 0.5)).toFixed(2));
    const openRank = parseFloat((1.5 + Math.random() * 0.5 + globalScore).toFixed(2));
    const totalContributors = new Set(commits.map(c => c.contributor_id)).size;

    return {
      hriData, 
      geoData: { countryData, diversityScore, pieData },
      heatmapData: heatmapGrid,
      globalScore, openRank, totalCommits: commits.length, totalContributors,
      rawCommits: commits,
      history, regionalHistory, radarData,
      hriCoverage: parseFloat((hriCoverage * 100).toFixed(0)),
      filteredContributors,
      rawData // 用于导出
    };
  }, [rawData, regionFilter]);
};

// --- UI 组件库 ---

const InfoTooltip = ({ content }) => (
  <span className="ml-1 text-gray-500 hover:text-cyan-400 cursor-help relative group inline-block align-middle">
    <HelpCircle size={14} />
    <div className="absolute left-1/2 bottom-full mb-2 transform -translate-x-1/2 invisible group-hover:visible 
                    w-64 p-3 bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded-lg shadow-xl z-50 leading-relaxed pointer-events-none">
      {content}
      <div className="absolute left-1/2 top-full -mt-1 transform -translate-x-1/2 border-4 border-transparent border-t-gray-900"></div>
    </div>
  </span>
);

const MetricCard = ({ title, value, icon: Icon, unit = '', color = 'text-cyan-400', subValue, info, loading }) => (
  <div className="bg-gray-800/40 backdrop-blur-md p-6 rounded-2xl border border-gray-700/50 hover:border-cyan-500/30 hover:bg-gray-800/60 transition-all duration-300 group">
    <div className="flex items-center justify-between mb-4">
      <h3 className="text-sm font-medium text-gray-400 flex items-center gap-2">
        {title} {info && <InfoTooltip content={info} />}
      </h3>
      <div className={`p-2.5 rounded-xl bg-gray-800 border border-gray-700 group-hover:border-${color.split('-')[1]}-500/50 transition-colors ${color}`}>
        <Icon size={18} />
      </div>
    </div>
    {loading ? (
        <div className="animate-pulse space-y-3">
            <div className="h-8 bg-gray-700 rounded w-24"></div>
            <div className="h-4 bg-gray-700/50 rounded w-16"></div>
        </div>
    ) : (
        <div className="flex flex-col gap-1">
            <p className={`text-3xl font-bold text-gray-100 tracking-tight`}>{value}<span className="text-lg text-gray-500 ml-1 font-normal">{unit}</span></p>
            {subValue && <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">{subValue}</p>}
        </div>
    )}
  </div>
);

const ChartCard = ({ title, icon: Icon, children, className, subtitle, action }) => (
    <div className={`bg-gray-800/80 backdrop-blur rounded-2xl border border-gray-700 p-6 shadow-xl flex flex-col ${className}`}>
        <div className="mb-6 flex items-start justify-between">
            <div>
                <h3 className="text-lg font-bold text-gray-100 flex items-center gap-2">
                    <Icon size={20} className="text-cyan-400" />
                    {title}
                </h3>
                {subtitle && <p className="text-xs text-gray-500 mt-1 ml-7">{subtitle}</p>}
            </div>
            {action && <div>{action}</div>}
        </div>
        <div className="flex-1 w-full min-h-0 relative">
            {children}
        </div>
    </div>
);

// --- 洞察生成器 ---
const InsightGenerator = ({ data, selectedProject, onGenerate, loading, insightText }) => (
  <div className="relative overflow-hidden bg-gradient-to-r from-gray-900 via-gray-800 to-gray-900 p-1 rounded-2xl shadow-2xl mt-6">
    <div className="absolute inset-0 bg-gradient-to-r from-cyan-500/20 via-blue-500/20 to-purple-500/20 opacity-50 blur-xl"></div>
    <div className="relative bg-[#0F1623] rounded-xl p-6 lg:p-8">
        <div className="flex flex-col lg:flex-row gap-8 items-start">
            <div className="lg:w-1/3 space-y-6">
                <div>
                    <h2 className="text-2xl font-bold text-white flex items-center gap-3">
                        <Sparkles className="text-yellow-400 fill-yellow-400" size={24} />
                        AI 智能顾问
                    </h2>
                    <p className="text-gray-400 mt-2 text-sm leading-relaxed">
                        深度解析当前时间段内的数据模式。获取针对性的全球化运营策略与改进建议。
                    </p>
                </div>
                
                <button
                    onClick={onGenerate}
                    disabled={loading || !API_KEY}
                    className={`w-full group relative flex items-center justify-center px-6 py-4 text-white font-bold rounded-xl shadow-lg transition-all overflow-hidden
                                ${loading 
                                    ? 'bg-gray-800 cursor-not-allowed' 
                                    : 'bg-gradient-to-r from-cyan-600 to-blue-600 hover:shadow-cyan-500/25'}`}
                >
                    <div className="absolute inset-0 bg-white/20 transform -skew-x-12 -translate-x-full group-hover:translate-x-full transition-transform duration-700"></div>
                    {loading ? (
                        <>
                            <div className="animate-spin rounded-full h-5 w-5 border-2 border-white/30 border-t-white mr-3"></div>
                            正在深度思考...
                        </>
                    ) : (
                        <>
                            <Activity size={20} className="mr-2" />
                            生成 {selectedProject} 诊断报告
                        </>
                    )}
                </button>
                {!API_KEY && <div className="p-3 bg-red-500/10 border border-red-500/20 rounded-lg text-xs text-red-400 flex items-center gap-2"><Target size={14}/> 提示：请配置 API Key 启用此功能</div>}
            </div>

            <div className="lg:w-2/3 min-h-[160px] bg-gray-900/50 rounded-xl border border-gray-800 p-6 relative">
                {insightText ? (
                    <div className="prose prose-invert prose-sm max-w-none">
                        <div className="whitespace-pre-wrap leading-relaxed text-gray-300 font-sans">
                             {insightText}
                        </div>
                    </div>
                ) : (
                    <div className="absolute inset-0 flex flex-col items-center justify-center text-gray-600 text-sm">
                        <Sparkles size={48} className="mb-4 opacity-20" />
                        <p>点击左侧按钮，AI 将为您解读数据...</p>
                    </div>
                )}
            </div>
        </div>
    </div>
  </div>
);

// --- 主应用 ---
const App = () => {
  const [selectedProject, setSelectedProject] = useState('Project-A');
  const [timeRange, setTimeRange] = useState('3M');
  const [regionFilter, setRegionFilter] = useState(null); // 新增：地区筛选
  const [loading, setLoading] = useState(true);
  
  // AI State
  const [insightLoading, setInsightLoading] = useState(false);
  const [insightText, setInsightText] = useState("");
  
  const data = useDataProcessor(selectedProject, timeRange, regionFilter);
  
  // 模拟数据重新获取的 Loading 效果
  useEffect(() => {
    setLoading(true);
    setRegionFilter(null); // 切换项目或时间时重置筛选
    setInsightText("");
    const timer = setTimeout(() => setLoading(false), 600);
    return () => clearTimeout(timer);
  }, [selectedProject, timeRange]);
  
  const generateInsight = useCallback(async () => {
    if (!API_KEY) {
      setInsightText("演示模式提示：请在代码中配置您的 Gemini API Key 以启用 AI 分析功能。");
      return;
    }
    setInsightLoading(true);
    setInsightText("");
    
    const rangeLabel = TIME_RANGES.find(r => r.value === timeRange)?.label;
    const metricsSummary = `
      项目: ${selectedProject} (时间范围: ${rangeLabel})
      [核心指标] 综合得分:${data.globalScore}, 24HRI覆盖:${data.hriCoverage}%, Geo熵:${data.geoData.diversityScore}, 贡献者数:${data.totalContributors}
      [五维雷达] ${data.radarData.map(r => `${r.subject}:${r.A}`).join(', ')}
      [地域Top5] ${data.geoData.pieData.slice(0,5).map(p => `${p.name}:${p.value}`).join(', ')}
    `;
    
    // 改进 Prompt: 要求 Markdown 格式
    const systemPrompt = `你是一位世界级的开源社区运营总监和数据分析师。请根据提供的多维数据，为 ${selectedProject} 写一份结构化的“全球化健康度诊断书”。
    
    要求格式如下（使用 Markdown）：
    ### 1. 核心洞察 (Executive Summary)
    用一两句话总结当前全球化状态。
    
    ### 2. 关键优势 (Strengths)
    列出2-3个数据表现亮眼的点。
    
    ### 3. 风险预警 (Risks)
    指出存在的隐患（如某地区过于集中、时区覆盖不足等）。
    
    ### 4. 战术建议 (Action Items)
    给出3条具体的下一步行动建议。
    
    语气专业、犀利、直接。`;
    
    try {
        const response = await fetch(`${API_URL_GEMINI}?key=${API_KEY}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                contents: [{ parts: [{ text: `分析数据：\n${metricsSummary}` }] }],
                systemInstruction: { parts: [{ text: systemPrompt }] },
            })
        });
        const result = await response.json();
        setInsightText(result.candidates?.[0]?.content?.parts?.[0]?.text || "分析返回为空。");
    } catch (e) {
        setInsightText(`API调用失败: ${e.message}`);
    }
    setInsightLoading(false);
  }, [selectedProject, data, timeRange]);
  
  // 导出功能
  const handleExport = () => {
      const exportData = {
          project: selectedProject,
          timeRange,
          generatedAt: new Date().toISOString(),
          metrics: {
              globalScore: data.globalScore,
              totalCommits: data.totalCommits
          },
          summaryData: data.rawCommits // 模拟导出原始数据
      };
      const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${selectedProject}_report_${timeRange}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
  };

  // Recharts Event Handlers
  const handlePieClick = (data) => {
      if (data && data.name && data.name !== 'Others') {
          setRegionFilter(prev => prev === data.name ? null : data.name);
      }
  };

  if (!data && loading) return <div className="min-h-screen bg-[#0B1120] flex items-center justify-center text-cyan-500">Loading...</div>;

  return (
    <div className="min-h-screen bg-[#0B1120] text-gray-100 font-sans selection:bg-cyan-500/30 selection:text-cyan-200 pb-12">
      
      {/* 顶部导航 */}
      <nav className="border-b border-gray-800 bg-[#0B1120]/80 backdrop-blur-md sticky top-0 z-50">
        <div className="max-w-[1920px] mx-auto px-6 h-16 flex items-center justify-between">
            <div className="flex items-center gap-4">
                <div className="bg-gradient-to-br from-cyan-500 to-blue-600 w-8 h-8 rounded-lg flex items-center justify-center shadow-lg shadow-cyan-500/20">
                    <Globe size={18} className="text-white" />
                </div>
                <div>
                    <h1 className="text-lg font-bold text-white tracking-tight flex items-center gap-2">
                        GlobalPulse <span className="text-cyan-500">Analytics Pro</span>
                        <span className="text-[10px] bg-cyan-900/50 text-cyan-400 px-1.5 py-0.5 rounded border border-cyan-800">v2.0</span>
                    </h1>
                </div>
            </div>
            
            <div className="flex items-center gap-4">
                 {/* 时间筛选器 */}
                 <div className="bg-gray-900 p-1 rounded-lg border border-gray-700 flex items-center">
                    {TIME_RANGES.map(range => (
                        <button
                            key={range.value}
                            onClick={() => setTimeRange(range.value)}
                            className={`px-3 py-1 text-xs font-medium rounded-md transition-all ${
                                timeRange === range.value 
                                ? 'bg-gray-700 text-cyan-400 shadow-sm' 
                                : 'text-gray-500 hover:text-gray-300'
                            }`}
                        >
                            {range.label}
                        </button>
                    ))}
                 </div>

                 <div className="h-6 w-px bg-gray-700 mx-2"></div>

                {/* 项目选择器 */}
                <div className="relative group">
                    <select
                        value={selectedProject}
                        onChange={(e) => setSelectedProject(e.target.value)}
                        className="bg-gray-900 border border-gray-700 text-gray-200 py-1.5 pl-3 pr-8 rounded-lg text-sm focus:ring-1 focus:ring-cyan-500 appearance-none cursor-pointer hover:border-gray-600 transition-colors"
                    >
                        <option value="Project-A">Project-A</option>
                        <option value="Project-B">Project-B</option>
                    </select>
                    <ChevronDown size={14} className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none"/>
                </div>

                <button 
                    onClick={handleExport}
                    className="p-2 text-gray-400 hover:text-cyan-400 bg-gray-900 border border-gray-700 rounded-lg hover:border-cyan-500/50 transition-all"
                    title="导出数据"
                >
                    <Download size={16} />
                </button>
            </div>
        </div>
      </nav>

      <main className="max-w-[1920px] mx-auto p-6 lg:p-8 space-y-6">
        
        {/* KPI Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
            <MetricCard
                title="全球化综合得分"
                value={data.globalScore.toFixed(2)}
                unit="/1.0"
                icon={Activity}
                subValue={`OpenRank: ${data.openRank}`}
                info="结合时间覆盖率和地理多样性的加权综合评分。"
                color={data.globalScore > 0.7 ? "text-emerald-400" : "text-amber-400"}
                loading={loading}
            />
            <MetricCard
                title="24HRI 覆盖率"
                value={`${data.hriCoverage}`}
                unit="%"
                icon={Clock}
                subValue="时区效率"
                info="项目在24小时周期内的活动覆盖比例。"
                color="text-cyan-400"
                loading={loading}
            />
            <MetricCard
                title="地理多样性 (Geo)"
                value={data.geoData.diversityScore.toFixed(2)}
                icon={MapIcon}
                subValue={`${data.geoData.countryData.length} 个活跃地区`}
                info="地理位置分布的熵值。"
                color="text-purple-400"
                loading={loading}
            />
            <MetricCard
                title="独立贡献者"
                value={data.totalContributors}
                icon={Users}
                subValue={`总提交量: ${data.totalCommits}`}
                info="选定时间范围内的去重贡献者数量。"
                color="text-pink-400"
                loading={loading}
            />
        </div>

        {/* Charts Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
            
            {/* 1. Trend */}
            <ChartCard title="历史趋势 (Trend)" icon={TrendingUp} className="xl:col-span-2 h-[400px]">
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={data.history} margin={{ top: 10, right: 30, left: 20, bottom: 0 }}>
                        <defs>
                            <linearGradient id="colorScore" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.3}/>
                                <stop offset="95%" stopColor="#06b6d4" stopOpacity={0}/>
                            </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                        <XAxis dataKey="label" stroke="#9CA3AF" tickLine={false} axisLine={false} />
                        <YAxis yAxisId="left" stroke="#06b6d4" domain={[0, 1]} tickLine={false} axisLine={false} />
                        <YAxis yAxisId="right" orientation="right" stroke="#8b5cf6" tickLine={false} axisLine={false} />
                        <Tooltip contentStyle={{ backgroundColor: '#111827', border: '1px solid #374151', borderRadius: '8px' }} />
                        <Legend iconType="circle" />
                        <Area yAxisId="left" type="monotone" dataKey="score" name="全球化得分" stroke="#06b6d4" fillOpacity={1} fill="url(#colorScore)" />
                        <Line yAxisId="right" type="monotone" dataKey="commits" name="提交量" stroke="#8b5cf6" strokeWidth={2} dot={{r: 4}} />
                    </AreaChart>
                </ResponsiveContainer>
            </ChartCard>

            {/* 2. Geo Pie (Interactive) */}
            <ChartCard 
                title="地域分布 (互动)" 
                icon={MapIcon} 
                subtitle="点击扇区筛选列表" 
                className="h-[400px]"
                action={
                    regionFilter && (
                        <button onClick={() => setRegionFilter(null)} className="flex items-center text-xs text-red-400 hover:text-red-300">
                            <XCircle size={12} className="mr-1"/> 重置筛选: {regionFilter}
                        </button>
                    )
                }
            >
                <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                        <Pie
                            data={data.geoData.pieData}
                            dataKey="value"
                            nameKey="name"
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={100}
                            paddingAngle={5}
                            onClick={handlePieClick}
                            className="cursor-pointer focus:outline-none"
                        >
                            {data.geoData.pieData.map((entry, index) => (
                                <Cell 
                                    key={`cell-${index}`} 
                                    fill={COLORS[index % COLORS.length]} 
                                    stroke={regionFilter === entry.name ? '#fff' : 'none'}
                                    strokeWidth={2}
                                    fillOpacity={regionFilter && regionFilter !== entry.name ? 0.3 : 1}
                                />
                            ))}
                        </Pie>
                        <Tooltip contentStyle={{ backgroundColor: '#111827', border: 'none', borderRadius: '8px' }} itemStyle={{ color: '#fff' }} />
                        <Legend layout="vertical" verticalAlign="middle" align="right" />
                    </PieChart>
                </ResponsiveContainer>
            </ChartCard>

            {/* 3. Heatmap */}
            <ChartCard title="协作脉冲 (Activity Pulse)" icon={Calendar} subtitle="UTC 时间周视图" className="xl:col-span-2 h-[350px]">
                 <ActivityHeatmap data={data.heatmapData} />
            </ChartCard>
            
            {/* 4. Contributor Leaderboard (Filtered) */}
            <ChartCard 
                title="核心贡献者" 
                icon={Award} 
                subtitle={regionFilter ? `已筛选: ${regionFilter}` : "Top 10 Contributors"} 
                className="h-[350px]"
                action={regionFilter ? <Filter size={16} className="text-cyan-400 animate-pulse"/> : null}
            >
                <div className="overflow-y-auto pr-2 custom-scrollbar max-h-[280px]">
                    <table className="w-full text-left border-collapse">
                        <thead>
                            <tr className="text-xs text-gray-500 border-b border-gray-700 sticky top-0 bg-gray-800/90 backdrop-blur">
                                <th className="py-2 font-medium">Rank</th>
                                <th className="py-2 font-medium">User</th>
                                <th className="py-2 font-medium">Loc</th>
                                <th className="py-2 font-medium text-right">Commits</th>
                            </tr>
                        </thead>
                        <tbody className="text-sm">
                            {data.filteredContributors.length > 0 ? data.filteredContributors.map((user, idx) => (
                                <tr key={user.id} className="group hover:bg-gray-700/30 transition-colors border-b border-gray-800/50 last:border-0">
                                    <td className="py-2.5 text-gray-500 w-8">{idx + 1}</td>
                                    <td className="py-2.5 font-medium text-gray-300 group-hover:text-white flex items-center gap-2">
                                        <div className="w-6 h-6 rounded-full bg-gradient-to-tr from-gray-700 to-gray-600 flex items-center justify-center text-[10px]">
                                            {user.name.charAt(0)}
                                        </div>
                                        {user.name}
                                    </td>
                                    <td className="py-2.5">
                                        <span className={`px-1.5 py-0.5 rounded text-[10px] font-mono border ${
                                            regionFilter === user.location 
                                            ? 'bg-cyan-900/40 border-cyan-700 text-cyan-400' 
                                            : 'bg-gray-800 border-gray-700 text-gray-400'
                                        }`}>
                                            {user.location}
                                        </span>
                                    </td>
                                    <td className="py-2.5 text-right font-mono text-cyan-400">{user.count}</td>
                                </tr>
                            )) : (
                                <tr>
                                    <td colSpan="4" className="py-8 text-center text-gray-500 text-sm">
                                        该地区暂无活跃贡献者
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </ChartCard>

            {/* 5. Radar */}
            <ChartCard title="健康度雷达" icon={Target} className="h-[400px]">
                <ResponsiveContainer width="100%" height="100%">
                    <RadarChart cx="50%" cy="50%" outerRadius="70%" data={data.radarData}>
                        <PolarGrid stroke="#374151" />
                        <PolarAngleAxis dataKey="subject" tick={{ fill: '#9CA3AF', fontSize: 12 }} />
                        <PolarRadiusAxis angle={30} domain={[0, 150]} tick={false} axisLine={false} />
                        <Radar name={selectedProject} dataKey="A" stroke={RADAR_COLOR} strokeWidth={2} fill={RADAR_COLOR} fillOpacity={0.3} />
                        <Tooltip contentStyle={{ backgroundColor: '#111827', border: 'none', borderRadius: '8px' }} />
                    </RadarChart>
                </ResponsiveContainer>
            </ChartCard>

            {/* 6. HRI Curve */}
            <ChartCard title="24小时分布" icon={Clock4} className="h-[400px]">
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={data.hriData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                        <defs>
                            <linearGradient id="colorCommits" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                                <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                            </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false}/>
                        <XAxis dataKey="hour" stroke="#9CA3AF" tickLine={false} axisLine={false} tickFormatter={(h) => `${h}H`} />
                        <Tooltip contentStyle={{ backgroundColor: '#111827', border: 'none', borderRadius: '8px' }} />
                        <Area type="monotone" dataKey="commits" stroke="#10b981" fillOpacity={1} fill="url(#colorCommits)" />
                    </AreaChart>
                </ResponsiveContainer>
            </ChartCard>

        </div>

        {/* AI Insight */}
        <InsightGenerator 
            data={data} 
            selectedProject={selectedProject} 
            onGenerate={generateInsight} 
            loading={insightLoading} 
            insightText={insightText} 
        />

      </main>
    </div>
  );
};

export default App;
