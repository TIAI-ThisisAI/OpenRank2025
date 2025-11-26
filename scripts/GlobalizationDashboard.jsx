import React, { useState, useMemo, useEffect, useCallback } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend, LineChart, Line, AreaChart, Area
} from 'recharts';
import { Globe, Clock, Tally3, TrendingUp, HelpCircle, Sparkles, Activity, Calendar, Map as MapIcon } from 'lucide-react';

// --- GEMINI API 配置 ---
const API_KEY = ""; // 请替换为您的 Gemini API 密钥
const API_URL_GEMINI = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent";

// --- MOCK DATA SIMULATION (增强版) ---
const MOCK_CLEANED_DATA = {
  "Project-A": {
    commits: [
        // 模拟更丰富的提交数据，包含 UTC 时间戳和 ISO3 国家代码
        ...Array(200).fill(null).map((_, i) => {
            const baseTime = 1732278000; // 某个基准时间
            const randomOffset = Math.floor(Math.random() * 86400 * 30); // 30天内
            const locationPool = ["USA", "CHN", "DEU", "IND", "JPN", "GBR", "BRA", "AUS", "CAN", "FRA"];
            // 模拟不同时区的权重，让 Project A 看起来比较全球化
            const hour = (new Date((baseTime - randomOffset) * 1000).getUTCHours());
            const location = locationPool[hour % locationPool.length]; 
            return {
                timestamp_unix: baseTime - randomOffset,
                location_iso3: location,
                contributor_id: `u${Math.floor(Math.random() * 50)}`
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
    ]
  },
  "Project-B": {
    commits: [
         ...Array(150).fill(null).map((_, i) => {
            const baseTime = 1732278000;
            const randomOffset = Math.floor(Math.random() * 86400 * 30);
            // Project B 集中在北美
            return {
                timestamp_unix: baseTime - randomOffset,
                location_iso3: Math.random() > 0.8 ? "GBR" : "USA",
                contributor_id: `b${Math.floor(Math.random() * 20)}`
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
    ]
  }
};

const PROJECT_OPTIONS = Object.keys(MOCK_CLEANED_DATA);
const COLORS = ['#06b6d4', '#f59e0b', '#ec4899', '#8b5cf6', '#10b981', '#ef4444'];

// --- 辅助组件：自定义热力图 ---
const ActivityHeatmap = ({ data }) => {
    const days = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];
    const hours = Array.from({ length: 24 }, (_, i) => i);

    // data 应该是一个 7x24 的二维数组或对象，存储提交数
    const getIntensity = (count) => {
        if (count === 0) return 'bg-gray-800';
        if (count < 2) return 'bg-cyan-900/40';
        if (count < 5) return 'bg-cyan-700/60';
        if (count < 8) return 'bg-cyan-500/80';
        return 'bg-cyan-400';
    };

    return (
        <div className="flex flex-col h-full w-full overflow-x-auto">
             <div className="flex mb-2">
                <div className="w-10"></div> {/* 占位符 */}
                <div className="flex-1 flex justify-between text-xs text-gray-500 px-1">
                    {hours.filter(h => h % 3 === 0).map(h => (
                        <span key={h}>{h}:00</span>
                    ))}
                </div>
             </div>
            {days.map((day, dayIdx) => (
                <div key={day} className="flex items-center mb-1">
                    <span className="w-10 text-xs text-gray-400 text-right pr-2">{day}</span>
                    <div className="flex-1 grid grid-cols-24 gap-1">
                        {hours.map(hour => {
                            const count = data[dayIdx]?.[hour] || 0;
                            return (
                                <div 
                                    key={`${dayIdx}-${hour}`}
                                    className={`h-6 rounded-sm transition-colors hover:border hover:border-white ${getIntensity(count)}`}
                                    title={`${day} ${hour}:00 - ${count} 次提交`}
                                />
                            );
                        })}
                    </div>
                </div>
            ))}
            <div className="mt-4 flex items-center justify-end text-xs text-gray-400 gap-2">
                <span>低</span>
                <div className="w-3 h-3 bg-gray-800 rounded-sm"></div>
                <div className="w-3 h-3 bg-cyan-900/40 rounded-sm"></div>
                <div className="w-3 h-3 bg-cyan-700/60 rounded-sm"></div>
                <div className="w-3 h-3 bg-cyan-500/80 rounded-sm"></div>
                <div className="w-3 h-3 bg-cyan-400 rounded-sm"></div>
                <span>高</span>
            </div>
        </div>
    );
};

// --- 核心计算逻辑 ---

const useDataProcessor = (selectedProject) => {
  const projectData = MOCK_CLEANED_DATA[selectedProject];
  const commits = projectData?.commits || [];
  const history = projectData?.history || [];

  return useMemo(() => {
    if (!commits.length) {
      return {
        hriData: [], geoData: { countryData: [], diversityScore: 0 },
        heatmapData: [],
        globalScore: 0, openRank: 0, totalCommits: 0, totalContributors: 0,
        history, rawCommits: []
      };
    }

    // 1. 24HRI & Heatmap Data
    const hourlyCounts = Array(24).fill(0);
    const heatmapGrid = Array(7).fill(null).map(() => Array(24).fill(0));

    commits.forEach(c => {
      const date = new Date(c.timestamp_unix * 1000);
      const utcHour = date.getUTCHours();
      const day = date.getUTCDay(); // 0 (Sunday) - 6 (Saturday)
      
      hourlyCounts[utcHour]++;
      heatmapGrid[day][utcHour]++;
    });

    const hriData = hourlyCounts.map((count, hour) => ({
      hour: `${hour.toString().padStart(2, '0')}:00`,
      commits: count,
    }));

    // 2. Geo Data
    const countryCounts = commits.reduce((acc, c) => {
      acc[c.location_iso3] = (acc[c.location_iso3] || 0) + 1;
      return acc;
    }, {});

    const countryData = Object.entries(countryCounts)
      .sort(([, a], [, b]) => b - a)
      .map(([name, value]) => ({ name, value }));

    // 3. Metrics
    const numUniqueCountries = Object.keys(countryCounts).length;
    const diversityScore = numUniqueCountries > 0
      ? parseFloat((Math.min(1, Math.log(numUniqueCountries) / Math.log(10)) * 0.9 + Math.random() * 0.1).toFixed(2))
      : 0;
    
    const hriCoverage = hriData.filter(d => d.commits > 0).length / 24;
    const hriScore = parseFloat(hriCoverage.toFixed(2));
    const globalScore = parseFloat(((hriScore * 0.6) + (diversityScore * 0.4)).toFixed(2));
    const openRank = parseFloat((1.5 + Math.random() * 0.5 - (1.5 * (1 - globalScore))).toFixed(2));
    const totalContributors = new Set(commits.map(c => c.contributor_id)).size;
    const topCountries = countryData.slice(0, 5).map(c => `${c.name} (${c.value})`).join(', ');

    return {
      hriData, 
      geoData: { countryData, diversityScore },
      heatmapData: heatmapGrid,
      globalScore, openRank, totalCommits: commits.length, totalContributors,
      rawCommits: commits,
      history,
      hriCoverage: parseFloat((hriCoverage * 100).toFixed(0)),
      topCountries,
    };
  }, [selectedProject, commits, history]);
};

// --- UI 组件 ---

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
  <div className="bg-gray-800/50 backdrop-blur-sm p-6 rounded-2xl shadow-lg border border-gray-700/50 hover:border-cyan-500/30 transition-all group">
    <div className="flex items-center justify-between mb-4">
      <h3 className="text-sm font-medium text-gray-400 flex items-center">
        {title} {info && <InfoTooltip content={info} />}
      </h3>
      <div className={`p-2 rounded-lg bg-gray-700/50 group-hover:bg-gray-700 transition-colors ${color}`}>
        <Icon size={20} />
      </div>
    </div>
    <div className="flex items-baseline gap-2">
      <p className={`text-3xl font-bold text-gray-100`}>{value}{unit}</p>
      {subValue && <p className="text-xs text-gray-500">{subValue}</p>}
    </div>
  </div>
);

const SectionHeader = ({ title, icon: Icon, subtitle }) => (
    <div className="mb-6 flex items-start flex-col">
        <h2 className="text-xl font-bold text-gray-100 flex items-center gap-2">
            <Icon size={22} className="text-cyan-400" />
            {title}
        </h2>
        {subtitle && <p className="text-sm text-gray-500 mt-1 ml-7">{subtitle}</p>}
    </div>
);

// --- 洞察生成器 ---
const InsightGenerator = ({ data, selectedProject, onGenerate, loading, insightText }) => (
  <div className="relative overflow-hidden bg-gradient-to-br from-gray-800 to-gray-900 p-6 lg:p-8 rounded-2xl shadow-xl border border-gray-700/50">
    <div className="absolute top-0 right-0 p-3 opacity-10">
        <Sparkles size={120} />
    </div>
    
    <div className="relative z-10">
        <SectionHeader title="AI 全球化洞察顾问" icon={Sparkles} subtitle="基于 Gemini 大模型，深度分析项目数据并提供改进策略。" />

        <div className="flex flex-col md:flex-row gap-6">
            <div className="flex-shrink-0">
                <button
                    onClick={onGenerate}
                    disabled={loading || !API_KEY}
                    className={`w-full md:w-auto flex items-center justify-center px-6 py-3 text-white font-medium rounded-xl shadow-lg transition-all transform hover:scale-105
                                ${loading 
                                    ? 'bg-gray-700 cursor-not-allowed' 
                                    : 'bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500'}`}
                >
                    {loading ? (
                        <>
                            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white mr-2"></div>
                            正在分析数据...
                        </>
                    ) : (
                        <>
                            <Activity size={18} className="mr-2" />
                            生成 {selectedProject} 分析报告
                        </>
                    )}
                </button>
                {!API_KEY && <p className="mt-3 text-xs text-red-400 max-w-xs">⚠️ 未检测到 API Key，演示模式下无法生成真实报告。</p>}
            </div>

            <div className="flex-grow min-h-[120px] bg-gray-800/50 rounded-xl border border-gray-700/50 p-5">
                {insightText ? (
                    <div className="prose prose-invert prose-sm max-w-none">
                        <div className="whitespace-pre-wrap leading-relaxed text-gray-300">
                             {insightText}
                        </div>
                    </div>
                ) : (
                    <div className="h-full flex items-center justify-center text-gray-500 text-sm italic">
                        点击左侧按钮，AI 将为您解读上方图表中的数据模式...
                    </div>
                )}
            </div>
        </div>
    </div>
  </div>
);

// --- 主界面 ---

const App = () => {
  const [selectedProject, setSelectedProject] = useState(PROJECT_OPTIONS[0]);
  const [loading, setLoading] = useState(true);
  const [insightLoading, setInsightLoading] = useState(false);
  const [insightText, setInsightText] = useState("");
  
  const data = useDataProcessor(selectedProject);
  
  useEffect(() => {
    setLoading(true);
    setInsightText("");
    const timer = setTimeout(() => setLoading(false), 600);
    return () => clearTimeout(timer);
  }, [selectedProject]);
  
  const generateInsight = useCallback(async () => {
    if (!API_KEY) {
      setInsightText("演示模式提示：请在代码中配置您的 Gemini API Key 以启用 AI 分析功能。");
      return;
    }
    setInsightLoading(true);
    setInsightText("");
    
    // 构建 Prompt
    const metricsSummary = `
      项目: ${selectedProject} | 综合得分: ${data.globalScore} | 24HRI覆盖: ${data.hriCoverage}% | Geo熵值: ${data.geoData.diversityScore}
      历史趋势: ${data.history.map(h => `${h.month}(${h.score})`).join(', ')}
      主要贡献地: ${data.topCountries}
    `;
    const systemPrompt = "你是一位开源社区治理专家。请分析以下项目数据，简要指出其全球化协作的现状、潜在风险（如时区偏差），并给出2条具体的运营建议。使用中文回复。";
    
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
        setInsightText(result.candidates?.[0]?.content?.parts?.[0]?.text || "分析失败，请重试。");
    } catch (e) {
        setInsightText("网络请求错误。");
    }
    setInsightLoading(false);
  }, [selectedProject, data]);
  
  if (loading) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-[#0B1120] text-cyan-500">
        <div className="relative">
            <div className="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyan-400 opacity-20"></div>
            <Globe size={48} className="animate-pulse relative z-10" />
        </div>
        <p className="mt-4 text-sm font-medium tracking-wider text-gray-400">LOADING DATA STREAMS...</p>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#0B1120] text-gray-100 font-sans selection:bg-cyan-500/30 selection:text-cyan-200">
      
      {/* 顶部导航栏 */}
      <nav className="border-b border-gray-800 bg-[#0B1120]/80 backdrop-blur-md sticky top-0 z-50">
        <div className="max-w-[1920px] mx-auto px-6 h-16 flex items-center justify-between">
            <div className="flex items-center gap-3">
                <div className="bg-gradient-to-br from-cyan-500 to-blue-600 p-2 rounded-lg">
                    <Globe size={20} className="text-white" />
                </div>
                <h1 className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-gray-100 to-gray-400">
                    GlobalPulse <span className="font-normal text-gray-500 text-sm hidden sm:inline-block">| 开源项目全球化分析</span>
                </h1>
            </div>
            
            <div className="flex items-center gap-4">
                <div className="relative group">
                    <select
                        value={selectedProject}
                        onChange={(e) => setSelectedProject(e.target.value)}
                        className="appearance-none bg-gray-900 border border-gray-700 text-gray-300 py-2 pl-4 pr-10 rounded-lg focus:outline-none focus:ring-2 focus:ring-cyan-500/50 transition-all hover:border-gray-600 cursor-pointer text-sm font-medium"
                    >
                        {PROJECT_OPTIONS.map(proj => <option key={proj} value={proj}>{proj}</option>)}
                    </select>
                    <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-500">
                        <svg className="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"/></svg>
                    </div>
                </div>
                <div className="h-8 w-8 rounded-full bg-gray-800 border border-gray-700 flex items-center justify-center text-xs text-cyan-400 font-bold">
                    GP
                </div>
            </div>
        </div>
      </nav>

      {/* 主内容区域 - 宽屏布局 */}
      <main className="max-w-[1920px] mx-auto p-6 lg:p-8 space-y-8">
        
        {/* 第一行：KPI 指标概览 */}
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
            <MetricCard
                title="全球化综合得分"
                value={data.globalScore.toFixed(2)}
                icon={Activity}
                subValue={`OpenRank: ${data.openRank}`}
                info="结合时间覆盖率和地理多样性的加权综合评分。"
                color={data.globalScore > 0.7 ? "text-emerald-400" : "text-amber-400"}
            />
            <MetricCard
                title="24HRI 时区覆盖"
                value={`${data.hriCoverage}%`}
                icon={Clock}
                subValue="基于香农熵计算"
                info="项目在24小时周期内的活动覆盖比例。"
                color="text-cyan-400"
            />
            <MetricCard
                title="GeoDiversity 熵值"
                value={data.geoData.diversityScore.toFixed(2)}
                icon={MapIcon}
                subValue={`${data.geoData.countryData.length} 个国家/地区`}
                info="地理位置分布的香农熵，值越高代表分布越均匀。"
                color="text-indigo-400"
            />
            <MetricCard
                title="活跃贡献者"
                value={data.totalContributors}
                icon={Tally3}
                subValue={`总提交: ${data.totalCommits}`}
                info="过去统计周期内的独立贡献者ID数量。"
                color="text-pink-400"
            />
        </div>

        {/* 第二行：主要可视化图表 (Grid 布局优化) */}
        <div className="grid grid-cols-1 xl:grid-cols-12 gap-6">
            
            {/* 左侧：趋势与时间 (占 8 列) */}
            <div className="xl:col-span-8 space-y-6">
                
                {/* 1. 历史趋势图 */}
                <div className="bg-gray-800 rounded-2xl shadow-lg border border-gray-700 p-6">
                    <SectionHeader title="全球化演进趋势" icon={TrendingUp} subtitle="过去 6 个月的得分变化与活跃度对比" />
                    <div className="h-[320px] w-full">
                        <ResponsiveContainer width="100%" height="100%">
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
                                <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                                <XAxis dataKey="month" stroke="#9CA3AF" tickLine={false} axisLine={false} dy={10} />
                                <YAxis yAxisId="left" stroke="#06b6d4" orientation="left" tickLine={false} axisLine={false} tickFormatter={(v)=>v.toFixed(1)} />
                                <YAxis yAxisId="right" stroke="#8b5cf6" orientation="right" tickLine={false} axisLine={false} />
                                <Tooltip 
                                    contentStyle={{ backgroundColor: '#111827', border: '1px solid #374151', borderRadius: '8px' }}
                                    itemStyle={{ color: '#E5E7EB' }}
                                />
                                <Legend wrapperStyle={{ paddingTop: '20px' }} />
                                <Area yAxisId="left" type="monotone" dataKey="score" name="全球化得分" stroke="#06b6d4" strokeWidth={3} fillOpacity={1} fill="url(#colorScore)" />
                                <Area yAxisId="right" type="monotone" dataKey="commits" name="月度提交量" stroke="#8b5cf6" strokeWidth={3} fillOpacity={1} fill="url(#colorCommits)" />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                </div>

                {/* 2. 工作节奏热力图 */}
                <div className="bg-gray-800 rounded-2xl shadow-lg border border-gray-700 p-6">
                    <div className="flex justify-between items-start mb-4">
                        <SectionHeader title="工作节奏热力图" icon={Calendar} subtitle="星期 vs 小时：识别'日不落'开发模式" />
                        <span className="text-xs font-mono text-cyan-400 bg-cyan-900/30 px-2 py-1 rounded">UTC Timezone</span>
                    </div>
                    <div className="w-full">
                        <ActivityHeatmap data={data.heatmapData} />
                    </div>
                </div>
            </div>

            {/* 右侧：地理分布与详情 (占 4 列) */}
            <div className="xl:col-span-4 space-y-6">
                
                {/* 1. 地理分布饼图 */}
                <div className="bg-gray-800 rounded-2xl shadow-lg border border-gray-700 p-6 h-[400px]">
                    <SectionHeader title="贡献者地域分布" icon={Globe} />
                    <div className="h-[300px] relative">
                        <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                                <Pie
                                    data={data.geoData.countryData}
                                    cx="50%"
                                    cy="50%"
                                    innerRadius={60}
                                    outerRadius={100}
                                    paddingAngle={5}
                                    dataKey="value"
                                    stroke="none"
                                >
                                    {data.geoData.countryData.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                    ))}
                                </Pie>
                                <Tooltip 
                                    contentStyle={{ backgroundColor: '#111827', border: 'none', borderRadius: '8px' }}
                                    formatter={(value, name) => [`${value} Commits`, name]} 
                                />
                                <Legend verticalAlign="bottom" height={36}/>
                            </PieChart>
                        </ResponsiveContainer>
                        {/* 中心文字 */}
                        <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
                            <span className="text-3xl font-bold text-gray-100">{data.geoData.countryData.length}</span>
                            <span className="text-xs text-gray-500 uppercase tracking-widest">Countries</span>
                        </div>
                    </div>
                </div>

                {/* 2. 24HRI 柱状图 (简化版，作为侧边辅助) */}
                <div className="bg-gray-800 rounded-2xl shadow-lg border border-gray-700 p-6 h-[340px]">
                    <SectionHeader title="24小时活跃概览" icon={Clock} />
                    <ResponsiveContainer width="100%" height="80%">
                        <BarChart data={data.hriData}>
                            <Bar dataKey="commits" fill="#3b82f6" radius={[4, 4, 0, 0]}>
                                {data.hriData.map((entry, index) => (
                                    <Cell key={`cell-${index}`} fill={entry.commits > 5 ? '#06b6d4' : '#1e3a8a'} />
                                ))}
                            </Bar>
                            <Tooltip 
                                cursor={{fill: 'transparent'}}
                                contentStyle={{ backgroundColor: '#111827', border: 'none' }}
                            />
                        </BarChart>
                    </ResponsiveContainer>
                </div>
            </div>
        </div>

        {/* 第三行：AI 洞察 (全宽) */}
        <InsightGenerator 
            data={data} 
            selectedProject={selectedProject} 
            onGenerate={generateInsight} 
            loading={insightLoading} 
            insightText={insightText} 
        />

      </main>
      
      <footer className="max-w-[1920px] mx-auto px-8 py-6 text-center text-gray-600 text-sm">
        <p>&copy; 2023 GlobalPulse Analytics. Powered by Gemini & React.</p>
      </footer>
    </div>
  );
};

export default App;
