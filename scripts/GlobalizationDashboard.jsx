import React, { useState, useMemo, useEffect, useCallback } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend, LineChart, Line, AreaChart, Area,
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis
} from 'recharts';
import { Globe, Clock, Tally3, TrendingUp, HelpCircle, Sparkles, Activity, Calendar, Map as MapIcon, Award, Users, Target, Zap, Clock4 } from 'lucide-react';

// --- GEMINI API 配置 ---
const API_KEY = ""; // 请替换为您的 Gemini API 密钥
const API_URL_GEMINI = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent";

// --- MOCK DATA SIMULATION (增强版 v3) ---
const MOCK_CLEANED_DATA = {
  "Project-A": {
    commits: [
        ...Array(200).fill(null).map((_, i) => {
            const baseTime = 1732278000;
            const randomOffset = Math.floor(Math.random() * 86400 * 30);
            const locationPool = ["USA", "CHN", "DEU", "IND", "JPN", "GBR", "BRA", "AUS", "CAN", "FRA"];
            const hour = (new Date((baseTime - randomOffset) * 1000).getUTCHours());
            const location = locationPool[hour % locationPool.length]; 
            return {
                timestamp_unix: baseTime - randomOffset,
                location_iso3: location,
                contributor_id: `u${Math.floor(Math.random() * 20)}`,
                contributor_name: `User-${Math.floor(Math.random() * 20)}`
            };
        })
    ],
    history: [
        { month: '6月', score: 0.55, commits: 120 },
        { month: '7月', score: 0.58, commits: 145 },
        { month: '8月', score: 0.62, commits: 130 },
        { month: '9月', score: 0.65, commits: 160 },
        { month: '10月', score: 0.72, commits: 190 },
        { month: '11月', score: 0.78, commits: 200 },
    ],
    regionalHistory: [
        { month: '6月', 'North America': 60, 'Asia': 20, 'Europe': 30, 'Others': 10 },
        { month: '7月', 'North America': 65, 'Asia': 35, 'Europe': 35, 'Others': 10 },
        { month: '8月', 'North America': 50, 'Asia': 40, 'Europe': 30, 'Others': 10 },
        { month: '9月', 'North America': 55, 'Asia': 55, 'Europe': 40, 'Others': 10 },
        { month: '10月', 'North America': 60, 'Asia': 70, 'Europe': 45, 'Others': 15 },
        { month: '11月', 'North America': 60, 'Asia': 80, 'Europe': 50, 'Others': 10 },
    ],
    radarData: [
        { subject: '时区覆盖', A: 120, fullMark: 150 },
        { subject: '地域多元性', A: 98, fullMark: 150 },
        { subject: '贡献连续性', A: 86, fullMark: 150 },
        { subject: '社区活跃度', A: 99, fullMark: 150 },
        { subject: '新人留存', A: 85, fullMark: 150 },
        { subject: '非英语母语', A: 65, fullMark: 150 },
    ]
  },
  "Project-B": {
    commits: [
         ...Array(150).fill(null).map((_, i) => {
            const baseTime = 1732278000;
            const randomOffset = Math.floor(Math.random() * 86400 * 30);
            return {
                timestamp_unix: baseTime - randomOffset,
                location_iso3: Math.random() > 0.8 ? "GBR" : "USA",
                contributor_id: `b${Math.floor(Math.random() * 10)}`,
                contributor_name: `Dev-${Math.floor(Math.random() * 10)}`
            };
        })
    ],
    history: [
        { month: '6月', score: 0.40, commits: 80 },
        { month: '7月', score: 0.42, commits: 90 },
        { month: '8月', score: 0.41, commits: 85 },
        { month: '9月', score: 0.43, commits: 100 },
        { month: '10月', score: 0.45, commits: 110 },
        { month: '11月', score: 0.44, commits: 150 },
    ],
    regionalHistory: [
        { month: '6月', 'North America': 70, 'Asia': 5, 'Europe': 5, 'Others': 0 },
        { month: '7月', 'North America': 80, 'Asia': 5, 'Europe': 5, 'Others': 0 },
        { month: '8月', 'North America': 75, 'Asia': 5, 'Europe': 5, 'Others': 0 },
        { month: '9月', 'North America': 90, 'Asia': 5, 'Europe': 5, 'Others': 0 },
        { month: '10月', 'North America': 95, 'Asia': 10, 'Europe': 5, 'Others': 0 },
        { month: '11月', 'North America': 130, 'Asia': 10, 'Europe': 10, 'Others': 0 },
    ],
    radarData: [
        { subject: '时区覆盖', A: 60, fullMark: 150 },
        { subject: '地域多元性', A: 40, fullMark: 150 },
        { subject: '贡献连续性', A: 110, fullMark: 150 },
        { subject: '社区活跃度', A: 90, fullMark: 150 },
        { subject: '新人留存', A: 70, fullMark: 150 },
        { subject: '非英语母语', A: 20, fullMark: 150 },
    ]
  }
};

const PROJECT_OPTIONS = Object.keys(MOCK_CLEANED_DATA);
const COLORS = ['#06b6d4', '#f59e0b', '#ec4899', '#8b5cf6', '#10b981', '#ef4444', '#f97316', '#a1a1aa'];
const RADAR_COLOR = '#06b6d4';

// --- 辅助组件 ---
const ActivityHeatmap = ({ data }) => {
    const days = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];
    const hours = Array.from({ length: 24 }, (_, i) => i);
    const getIntensity = (count) => {
        if (count === 0) return 'bg-gray-800';
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
                                    className={`h-5 rounded-[2px] transition-all hover:ring-1 hover:ring-white ${getIntensity(count)}`}
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

// --- 核心数据处理 ---
const useDataProcessor = (selectedProject) => {
  const projectData = MOCK_CLEANED_DATA[selectedProject];
  const commits = projectData?.commits || [];
  const history = projectData?.history || [];
  const regionalHistory = projectData?.regionalHistory || [];
  const radarData = projectData?.radarData || [];

  return useMemo(() => {
    if (!commits.length) {
      return {
        hriData: [], geoData: { countryData: [], diversityScore: 0, pieData: [] },
        heatmapData: [],
        globalScore: 0, openRank: 0, totalCommits: 0, totalContributors: 0,
        history, regionalHistory, radarData, rawCommits: [], topContributors: [],
        hriCoverage: 0, topCountriesStr: ""
      };
    }

    // 1. 24HRI & Heatmap
    const hourlyCounts = Array(24).fill(0);
    const heatmapGrid = Array(7).fill(null).map(() => Array(24).fill(0));
    // 2. Geo & Contributors
    const countryCounts = {};
    const contributorStats = {};

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
              name: c.contributor_name || c.contributor_id,
              count: 0,
              location: c.location_iso3 
          };
      }
      contributorStats[c.contributor_id].count++;
    });

    const hriData = hourlyCounts.map((count, hour) => ({
      hour: `${hour.toString().padStart(2, '0')}`,
      commits: count,
    }));

    const countryData = Object.entries(countryCounts)
      .sort(([, a], [, b]) => b - a)
      .map(([name, value]) => ({ name, value }));
      
    // 3. Pie Chart Data
    const top5Countries = countryData.slice(0, 5);
    const otherCommits = countryData.slice(5).reduce((sum, c) => sum + c.value, 0);

    const pieData = [
        ...top5Countries,
        // Only include "Other" if there are commits left
        ...(otherCommits > 0 ? [{ name: '其他地区', value: otherCommits }] : [])
    ].filter(d => d.value > 0);


    const topContributors = Object.values(contributorStats)
        .sort((a, b) => b.count - a.count)
        .slice(0, 8); // Top 8

    // Metrics
    const numUniqueCountries = Object.keys(countryCounts).length;
    const diversityScore = numUniqueCountries > 0
      ? parseFloat((Math.min(1, Math.log(numUniqueCountries) / Math.log(10)) * 0.9 + Math.random() * 0.1).toFixed(2))
      : 0;
    
    const hriCoverage = hriData.filter(d => d.commits > 0).length / 24;
    const hriScore = parseFloat(hriCoverage.toFixed(2));
    const globalScore = parseFloat(((hriScore * 0.6) + (diversityScore * 0.4)).toFixed(2));
    const openRank = parseFloat((1.5 + Math.random() * 0.5 - (1.5 * (1 - globalScore))).toFixed(2));
    const totalContributors = new Set(commits.map(c => c.contributor_id)).size;
    const topCountriesStr = countryData.slice(0, 5).map(c => `${c.name} (${c.value})`).join(', ');

    return {
      hriData, 
      geoData: { countryData, diversityScore, pieData },
      heatmapData: heatmapGrid,
      globalScore, openRank, totalCommits: commits.length, totalContributors,
      rawCommits: commits,
      history, regionalHistory, radarData,
      hriCoverage: parseFloat((hriCoverage * 100).toFixed(0)),
      topCountriesStr,
      topContributors
    };
  }, [selectedProject, commits, history, regionalHistory, radarData]);
};

// --- 辅助组件 ---
// ... (ActivityHeatmap remains the same)

// --- UI 组件库 ---

const InfoTooltip = ({ content }) => (
  <span className="ml-1 text-gray-500 hover:text-cyan-400 cursor-pointer relative group inline-block align-middle">
    <HelpCircle size={14} />
    <div className="absolute left-1/2 bottom-full mb-2 transform -translate-x-1/2 invisible group-hover:visible 
                    w-64 p-3 bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded-lg shadow-xl z-50 leading-relaxed">
      {content}
      <div className="absolute left-1/2 top-full -mt-1 transform -translate-x-1/2 border-4 border-transparent border-t-gray-900"></div>
    </div>
  </span>
);

const MetricCard = ({ title, value, icon: Icon, unit = '', color = 'text-cyan-400', subValue, info }) => (
  <div className="bg-gray-800/40 backdrop-blur-md p-6 rounded-2xl border border-gray-700/50 hover:border-cyan-500/30 hover:bg-gray-800/60 transition-all duration-300 group">
    <div className="flex items-center justify-between mb-4">
      <h3 className="text-sm font-medium text-gray-400 flex items-center gap-2">
        {title} {info && <InfoTooltip content={info} />}
      </h3>
      <div className={`p-2.5 rounded-xl bg-gray-800 border border-gray-700 group-hover:border-${color.split('-')[1]}-500/50 transition-colors ${color}`}>
        <Icon size={18} />
      </div>
    </div>
    <div className="flex flex-col gap-1">
      <p className={`text-3xl font-bold text-gray-100 tracking-tight`}>{value}<span className="text-lg text-gray-500 ml-1 font-normal">{unit}</span></p>
      {subValue && <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">{subValue}</p>}
    </div>
  </div>
);

const ChartCard = ({ title, icon: Icon, children, className, subtitle }) => (
    <div className={`bg-gray-800 rounded-2xl border border-gray-700 p-6 shadow-xl flex flex-col ${className}`}>
        <div className="mb-6 flex items-start justify-between">
            <div>
                <h3 className="text-lg font-bold text-gray-100 flex items-center gap-2">
                    <Icon size={20} className="text-cyan-400" />
                    {title}
                </h3>
                {subtitle && <p className="text-xs text-gray-500 mt-1 ml-7">{subtitle}</p>}
            </div>
            {/* Optional Actions */}
        </div>
        <div className="flex-1 w-full min-h-0 relative">
            {children}
        </div>
    </div>
);

// --- 洞察生成器 ---
const InsightGenerator = ({ data, selectedProject, onGenerate, loading, insightText }) => (
  <div className="relative overflow-hidden bg-gradient-to-r from-gray-900 via-gray-800 to-gray-900 p-1 rounded-2xl shadow-2xl">
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
                        基于 Gemini 大模型引擎，深度解析上方图表中的复杂数据模式。获取针对性的全球化运营策略与改进建议。
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
                            生成 {selectedProject} 深度诊断报告
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
  const [selectedProject, setSelectedProject] = useState(PROJECT_OPTIONS[0]);
  const [loading, setLoading] = useState(true);
  const [insightLoading, setInsightLoading] = useState(false);
  const [insightText, setInsightText] = useState("");
  
  const data = useDataProcessor(selectedProject);
  
  useEffect(() => {
    setLoading(true);
    setInsightText("");
    const timer = setTimeout(() => setLoading(false), 800);
    return () => clearTimeout(timer);
  }, [selectedProject]);
  
  const generateInsight = useCallback(async () => {
    if (!API_KEY) {
      setInsightText("演示模式提示：请在代码中配置您的 Gemini API Key 以启用 AI 分析功能。");
      return;
    }
    setInsightLoading(true);
    setInsightText("");
    
    // 构建丰富的 Prompt
    const metricsSummary = `
      项目: ${selectedProject}
      [核心指标] 综合得分:${data.globalScore}, 24HRI:${data.hriCoverage}%, Geo熵:${data.geoData.diversityScore}
      [五维雷达] ${data.radarData.map(r => `${r.subject}:${r.A}`).join(', ')}
      [地域集中度] Top5国家贡献占比高。
      [历史趋势] 全球化得分在持续上升，但贡献量的波动较大。
    `;
    const systemPrompt = "你是一位资深的开源社区运营总监。请根据提供的多维数据，写一份简短精悍的'全球化健康度诊断书'。包含：1. 核心优势（如时区覆盖是否良好）。2. 警示信号（如是否存在区域衰退或集中度过高）。3. 下一步行动建议。语气专业、犀利。";
    
    // Retry logic (Exponential Backoff)
    const MAX_RETRIES = 3;
    let lastError = null;

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        try {
            const response = await fetch(`${API_URL_GEMINI}?key=${API_KEY}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    contents: [{ parts: [{ text: `分析数据：\n${metricsSummary}` }] }],
                    systemInstruction: { parts: [{ text: systemPrompt }] },
                })
            });

            if (!response.ok) {
                // If response is not OK, it might be a rate limit error (429)
                throw new Error(`API returned status ${response.status}`);
            }

            const result = await response.json();
            setInsightText(result.candidates?.[0]?.content?.parts?.[0]?.text || "分析失败，请重试。");
            lastError = null; // Clear error on success
            break; // Exit loop on success

        } catch (e) {
            lastError = e;
            if (attempt < MAX_RETRIES - 1) {
                const delay = Math.pow(2, attempt) * 1000;
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }
    
    if (lastError) {
        setInsightText(`网络请求错误或API调用失败。错误信息: ${lastError.message}`);
    }

    setInsightLoading(false);
  }, [selectedProject, data]);
  
  if (loading) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-[#0B1120] text-cyan-500 font-sans">
        <div className="relative w-24 h-24">
            <div className="absolute inset-0 border-t-4 border-cyan-500 rounded-full animate-spin"></div>
            <div className="absolute inset-3 border-t-4 border-blue-500 rounded-full animate-spin animation-delay-150"></div>
            <div className="absolute inset-0 flex items-center justify-center">
                <Globe size={32} className="text-gray-100 animate-pulse" />
            </div>
        </div>
        <p className="mt-6 text-sm font-medium tracking-[0.2em] text-gray-400 uppercase">Initializing Dashboard</p>
      </div>
    );
  }

  // Custom tooltips for Recharts
  const renderCommitTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const commitData = payload.find(p => p.dataKey === 'commits');
      const scoreData = payload.find(p => p.dataKey === 'score');
      
      // 确保 scoreData 和 commitData 都存在，防止读取 undefined 的 value 属性
      if (commitData && scoreData) {
          return (
            <div className="p-3 bg-gray-900 border border-gray-700 text-xs text-gray-200 rounded-lg shadow-xl">
              <p className="font-bold mb-1">{label}</p>
              <p className="text-cyan-400">得分: {scoreData.value !== undefined ? scoreData.value.toFixed(2) : 'N/A'}</p>
              <p className="text-purple-400">提交量: {commitData.value !== undefined ? commitData.value : 'N/A'}</p>
            </div>
          );
      }
    }
    return null;
  };
  
  const renderPieLabel = ({ name, percent }) => `${name} (${(percent * 100).toFixed(0)}%)`;

  return (
    <div className="min-h-screen bg-[#0B1120] text-gray-100 font-sans selection:bg-cyan-500/30 selection:text-cyan-200 pb-12">
      
      {/* 顶部导航 */}
      <nav className="border-b border-gray-800 bg-[#0B1120]/80 backdrop-blur-md sticky top-0 z-50">
        <div className="max-w-[1920px] mx-auto px-6 h-20 flex items-center justify-between">
            <div className="flex items-center gap-4">
                <div className="bg-gradient-to-br from-cyan-500 to-blue-600 w-10 h-10 rounded-xl flex items-center justify-center shadow-lg shadow-cyan-500/20">
                    <Globe size={22} className="text-white" />
                </div>
                <div>
                    <h1 className="text-xl font-bold text-white tracking-tight">
                        GlobalPulse <span className="text-cyan-500">Analytics</span>
                    </h1>
                    <p className="text-[10px] text-gray-500 uppercase tracking-widest font-medium">Open Source Intelligence</p>
                </div>
            </div>
            
            <div className="flex items-center gap-6">
                <div className="relative group">
                    <div className="absolute -inset-0.5 bg-gradient-to-r from-cyan-500 to-blue-500 rounded-lg blur opacity-30 group-hover:opacity-75 transition duration-200"></div>
                    <select
                        value={selectedProject}
                        onChange={(e) => setSelectedProject(e.target.value)}
                        className="relative bg-gray-900 border border-gray-700 text-gray-200 py-2.5 pl-4 pr-10 rounded-lg focus:outline-none focus:ring-1 focus:ring-cyan-500 transition-all cursor-pointer text-sm font-medium w-48 appearance-none"
                    >
                        {PROJECT_OPTIONS.map(proj => <option key={proj} value={proj}>{proj}</option>)}
                    </select>
                    <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center px-3 text-gray-400">
                         <MapIcon size={14} />
                    </div>
                </div>
            </div>
        </div>
      </nav>

      <main className="max-w-[1920px] mx-auto p-6 lg:p-8 space-y-8">
        
        {/* Row 1: KPI Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
            <MetricCard
                title="Global Score"
                value={data.globalScore.toFixed(2)}
                unit="/1.0"
                icon={Activity}
                subValue={`OpenRank: ${data.openRank}`}
                info="结合时间覆盖率和地理多样性的加权综合评分。"
                color={data.globalScore > 0.7 ? "text-emerald-400" : "text-amber-400"}
            />
            <MetricCard
                title="24HRI Coverage"
                value={`${data.hriCoverage}`}
                unit="%"
                icon={Clock}
                subValue="Timezone Efficiency"
                info="项目在24小时周期内的活动覆盖比例。"
                color="text-cyan-400"
            />
            <MetricCard
                title="Geo Diversity"
                value={data.geoData.diversityScore.toFixed(2)}
                icon={MapIcon}
                subValue={`${data.geoData.countryData.length} Regions Active`}
                info="地理位置分布的香农熵。"
                color="text-purple-400"
            />
            <MetricCard
                title="Contributors"
                value={data.totalContributors}
                icon={Users}
                subValue={`${data.totalCommits} Commits Total`}
                info="过去统计周期内的独立贡献者ID数量。"
                color="text-pink-400"
            />
        </div>

        {/* Row 2: Trend & Concentration Analysis (NEW ROW) */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 h-[400px]">
            {/* 1. Score & Commit Trend */}
            <ChartCard title="历史趋势对比 (Score & Commit Trend)" icon={TrendingUp} subtitle="全球化得分 vs. 月度提交量" className="lg:col-span-2">
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={data.history} margin={{ top: 10, right: 30, left: 20, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                        <XAxis dataKey="month" stroke="#9CA3AF" tickLine={false} axisLine={false} />
                        <YAxis yAxisId="left" orientation="left" stroke="#06b6d4" domain={[0.3, 1]} tickLine={false} axisLine={false} />
                        <YAxis yAxisId="right" orientation="right" stroke="#8b5cf6" tickLine={false} axisLine={false} />
                        {/* 使用修正后的 renderCommitTooltip */}
                        <Tooltip content={renderCommitTooltip} /> 
                        <Legend wrapperStyle={{ paddingTop: '10px' }} iconType="circle" />
                        <Area yAxisId="left" type="monotone" dataKey="score" name="全球化得分" stroke="#06b6d4" fill="#06b6d4" fillOpacity={0.2} activeDot={{ r: 8 }} />
                        <Line yAxisId="right" type="monotone" dataKey="commits" name="提交量" stroke="#8b5cf6" fill="#8b5cf6" activeDot={{ r: 8 }} />
                    </AreaChart>
                </ResponsiveContainer>
            </ChartCard>

            {/* 2. Geographic Concentration (Pie Chart) */}
            <ChartCard title="地域集中度" icon={MapIcon} subtitle="Top 5国家贡献占比" className="lg:col-span-1">
                <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                        <Pie
                            data={data.geoData.pieData}
                            dataKey="value"
                            nameKey="name"
                            cx="50%"
                            cy="50%"
                            outerRadius={120}
                            fill="#8884d8"
                            labelLine={false}
                            label={renderPieLabel}
                        >
                            {data.geoData.pieData.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                            ))}
                        </Pie>
                        <Tooltip 
                            formatter={(value, name) => [value, name]}
                            contentStyle={{ backgroundColor: '#111827', border: '1px solid #374151', borderRadius: '8px' }}
                        />
                        <Legend wrapperStyle={{ paddingTop: '10px' }} iconType="circle" layout="vertical" align="right" verticalAlign="middle" />
                    </PieChart>
                </ResponsiveContainer>
            </ChartCard>
        </div>


        {/* Row 3: Main Analysis Grid (Re-numbered) */}
        <div className="grid grid-cols-1 xl:grid-cols-12 gap-6 h-auto">
            
            {/* Left Column (Main) - 8 Cols */}
            <div className="xl:col-span-8 flex flex-col gap-6">
                
                {/* 1. Regional Evolution (Stacked Bar) */}
                <ChartCard title="区域贡献演进 (Regional Evolution)" icon={TrendingUp} subtitle="过去半年各大洲贡献量堆叠趋势" className="h-[380px]">
                    <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={data.regionalHistory} margin={{ top: 20, right: 30, left: 0, bottom: 5 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                            <XAxis dataKey="month" stroke="#9CA3AF" tickLine={false} axisLine={false} />
                            <YAxis stroke="#9CA3AF" tickLine={false} axisLine={false} />
                            <Tooltip 
                                contentStyle={{ backgroundColor: '#111827', border: '1px solid #374151', borderRadius: '8px' }}
                                cursor={{fill: '#1F2937'}}
                            />
                            <Legend wrapperStyle={{ paddingTop: '10px' }} iconType="circle" />
                            <Bar dataKey="North America" stackId="a" fill="#3b82f6" radius={[0,0,0,0]} barSize={40} />
                            <Bar dataKey="Asia" stackId="a" fill="#06b6d4" />
                            <Bar dataKey="Europe" stackId="a" fill="#8b5cf6" />
                            <Bar dataKey="Others" stackId="a" fill="#64748b" radius={[4,4,0,0]} />
                        </BarChart>
                    </ResponsiveContainer>
                </ChartCard>

                {/* 2. HRI Distribution (Line Chart) - NEW */}
                <ChartCard title="24小时活动分布 (HRI Distribution)" icon={Clock4} subtitle="UTC 时间轴：识别活动高峰和低谷" className="h-[300px]">
                    <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={data.hriData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                            <XAxis 
                                dataKey="hour" 
                                stroke="#9CA3AF" 
                                tickLine={false} 
                                axisLine={false} 
                                interval={2} 
                                tickFormatter={(h) => `${h}H`}
                            />
                            <YAxis stroke="#9CA3AF" tickLine={false} axisLine={false} />
                            <Tooltip 
                                contentStyle={{ backgroundColor: '#111827', border: '1px solid #374151', borderRadius: '8px' }}
                                labelFormatter={(h) => `UTC ${h}:00`}
                            />
                            <Line type="monotone" dataKey="commits" name="提交量" stroke="#10b981" strokeWidth={3} dot={{ stroke: '#10b981', strokeWidth: 2, r: 4 }} activeDot={{ r: 8 }} />
                        </LineChart>
                    </ResponsiveContainer>
                </ChartCard>

                {/* 3. Heatmap */}
                <ChartCard title="协作脉冲热力图 (Activity Pulse)" icon={Calendar} subtitle="UTC 时间周视图：识别跨时区协作模式" className="h-[280px]">
                     <ActivityHeatmap data={data.heatmapData} />
                </ChartCard>
            </div>

            {/* Right Column (Details) - 4 Cols */}
            <div className="xl:col-span-4 flex flex-col gap-6">
                
                {/* 1. Globalization Radar */}
                <ChartCard title="全球化健康雷达" icon={Target} subtitle="五维诊断模型" className="h-[350px]">
                    <ResponsiveContainer width="100%" height="100%">
                        <RadarChart cx="50%" cy="50%" outerRadius="70%" data={data.radarData}>
                            <PolarGrid stroke="#374151" />
                            <PolarAngleAxis dataKey="subject" tick={{ fill: '#9CA3AF', fontSize: 12 }} />
                            <PolarRadiusAxis angle={30} domain={[0, 150]} tick={false} axisLine={false} />
                            <Radar
                                name={selectedProject}
                                dataKey="A"
                                stroke={RADAR_COLOR}
                                strokeWidth={2}
                                fill={RADAR_COLOR}
                                fillOpacity={0.3}
                            />
                            <Tooltip 
                                contentStyle={{ backgroundColor: '#111827', border: 'none', borderRadius: '8px' }}
                                itemStyle={{ color: RADAR_COLOR }}
                            />
                        </RadarChart>
                    </ResponsiveContainer>
                </ChartCard>

                {/* 2. Contributor Leaderboard */}
                <ChartCard title="核心贡献者名人堂" icon={Award} subtitle="Top Contributors" className="flex-1 min-h-[300px]">
                    <div className="overflow-y-auto pr-2 custom-scrollbar max-h-[300px]">
                        <table className="w-full text-left border-collapse">
                            <thead>
                                <tr className="text-xs text-gray-500 border-b border-gray-700">
                                    <th className="py-2 font-medium">Rank</th>
                                    <th className="py-2 font-medium">User</th>
                                    <th className="py-2 font-medium">Region</th>
                                    <th className="py-2 font-medium text-right">Commits</th>
                                </tr>
                            </thead>
                            <tbody className="text-sm">
                                {data.topContributors.map((user, idx) => (
                                    <tr key={user.id} className="group hover:bg-gray-700/30 transition-colors border-b border-gray-800/50 last:border-0">
                                        <td className="py-3">
                                            <span className={`inline-flex items-center justify-center w-6 h-6 rounded-full text-xs font-bold 
                                                ${idx === 0 ? 'bg-yellow-500/20 text-yellow-400' : 
                                                  idx === 1 ? 'bg-gray-400/20 text-gray-300' : 
                                                  idx === 2 ? 'bg-orange-700/20 text-orange-400' : 'text-gray-600'}`}>
                                                {idx + 1}
                                            </span>
                                        </td>
                                        <td className="py-3 font-medium text-gray-300 group-hover:text-white">{user.name}</td>
                                        <td className="py-3">
                                            <span className="px-2 py-0.5 rounded text-[10px] font-mono bg-gray-800 border border-gray-700 text-gray-400">
                                                {user.location}
                                            </span>
                                        </td>
                                        <td className="py-3 text-right font-mono text-cyan-400">{user.count}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </ChartCard>
            </div>
        </div>

        {/* Row 4: AI Insight (Full Width) */}
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
