import React, { useState, useMemo, useEffect, useCallback } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend
} from 'recharts';
import { Globe, Clock, Tally3, TrendingUp, HelpCircle, Sparkles } from 'lucide-react';

// --- GEMINI API 配置 ---
const API_KEY = ""; // 请替换为您的 Gemini API 密钥
const API_URL_GEMINI = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent";

// --- MOCK DATA SIMULATION ---
// 模拟优化的数据结构：已清洗并标准化 (ISO3) 的提交记录
const MOCK_CLEANED_DATA = {
  "Project-A": [
    // UTC Hour: 0-23
    { timestamp_unix: 1732278000 + 3600 * 9, location_iso3: "USA", contributor_id: "u1" }, // 9h UTC
    { timestamp_unix: 1732296000 + 3600 * 14, location_iso3: "CHN", contributor_id: "u2" }, // 14h UTC
    { timestamp_unix: 1732317600 + 3600 * 20, location_iso3: "DEU", contributor_id: "u3" }, // 20h UTC
    { timestamp_unix: 1732281600 + 3600 * 10, location_iso3: "USA", contributor_id: "u1" }, // 10h UTC
    { timestamp_unix: 1732281600 + 3600 * 10, location_iso3: "CHN", contributor_id: "u4" },
    { timestamp_unix: 1732278000 + 3600 * 1, location_iso3: "RUS", contributor_id: "u5" }, // 1h UTC
    { timestamp_unix: 1732317600 + 3600 * 22, location_iso3: "IND", contributor_id: "u6" }, // 22h UTC
    { timestamp_unix: 1732296000 + 3600 * 14, location_iso3: "CHN", contributor_id: "u7" }, 
    { timestamp_unix: 1732278000 + 3600 * 9, location_iso3: "USA", contributor_id: "u8" }, 
    { timestamp_unix: 1732281600 + 3600 * 10, location_iso3: "USA", contributor_id: "u9" },
    { timestamp_unix: 1732281600 + 3600 * 11, location_iso3: "DEU", contributor_id: "u10" },
    { timestamp_unix: 1732317600 + 3600 * 19, location_iso3: "BRA", contributor_id: "u11" },
    ...Array(30).fill(null).map((_, i) => ({
      timestamp_unix: 1732278000 + 3600 * (i % 24),
      location_iso3: ["USA", "CHN", "DEU", "IND", "JPN", "GBR"][i % 6],
      contributor_id: `u${12 + i}`
    }))
  ].slice(0, 40), // 限制数量，避免数据过多
  "Project-B": [
    ...Array(50).fill(null).map((_, i) => ({
      timestamp_unix: 1732278000 + 3600 * (i % 24),
      location_iso3: ["USA", "USA", "USA", "USA", "CHN", "CHN", "IND"][i % 7],
      contributor_id: `b${1 + i}`
    }))
  ]
};

const PROJECT_OPTIONS = Object.keys(MOCK_CLEANED_DATA);
const COLORS = ['#00C49F', '#FFBB28', '#FF8042', '#0088FE', '#AF19FF', '#FF006E'];

// --- 核心计算逻辑 ---

/**
 * 提取 24HRI (24-Hour Coverage Index)
 * 计算每个 UTC 小时内的提交次数
 */
const calculate24HRI = (commits) => {
  const hourlyCounts = Array(24).fill(0);
  commits.forEach(c => {
    const date = new Date(c.timestamp_unix * 1000);
    const utcHour = date.getUTCHours();
    hourlyCounts[utcHour]++;
  });

  return hourlyCounts.map((count, hour) => ({
    hour: `${hour.toString().padStart(2, '0')}:00`,
    commits: count,
  }));
};

/**
 * 提取地理多样性指标
 * 计算国家分布和 GeoDiversity Score (模拟)
 */
const calculateGeoDiversity = (commits) => {
  const countryCounts = commits.reduce((acc, c) => {
    acc[c.location_iso3] = (acc[c.location_iso3] || 0) + 1;
    return acc;
  }, {});

  const data = Object.entries(countryCounts)
    .sort(([, a], [, b]) => b - a)
    .map(([name, value]) => ({ name, value }));

  // 模拟 GeoDiversity Score (基于国家数量和均匀度)
  const numUniqueCountries = Object.keys(countryCounts).length;
  const diversityScore = numUniqueCountries > 0
    ? parseFloat((Math.min(1, Math.log(numUniqueCountries) / Math.log(10)) * 0.9 + Math.random() * 0.1).toFixed(2))
    : 0;

  return { countryData: data, diversityScore };
};

/**
 * Hook 处理数据加载和计算
 */
const useDataProcessor = (selectedProject) => {
  const commits = MOCK_CLEANED_DATA[selectedProject] || [];

  const processedData = useMemo(() => {
    if (!commits.length) {
      return {
        hriData: [], geoData: { countryData: [], diversityScore: 0 },
        globalScore: 0, openRank: 0, totalCommits: 0, totalContributors: 0
      };
    }

    const hriData = calculate24HRI(commits);
    const { countryData, diversityScore } = calculateGeoDiversity(commits);
    const totalCommits = commits.length;
    const totalContributors = new Set(commits.map(c => c.contributor_id)).size;

    // 模拟全局得分 (Global Score) 计算
    const hriCoverage = hriData.filter(d => d.commits > 0).length / 24;
    const hriScore = parseFloat(hriCoverage.toFixed(2));
    const globalScore = parseFloat(((hriScore * 0.6) + (diversityScore * 0.4)).toFixed(2));
    
    // 模拟 OpenRank
    const openRank = parseFloat((1.5 + Math.random() * 0.5 - (1.5 * (1 - globalScore))).toFixed(2));

    const topCountries = countryData.slice(0, 5).map(c => `${c.name} (${c.value} 次提交)`).join(', ');


    return {
      hriData, geoData: { countryData, diversityScore },
      globalScore, openRank, totalCommits, totalContributors,
      rawCommits: commits,
      hriCoverage: parseFloat((hriCoverage * 100).toFixed(0)), // Percentage
      topCountries,
    };
  }, [selectedProject, commits]);

  return processedData;
};

// --- UI 组件 ---

// 帮助信息提示
const InfoTooltip = ({ content }) => (
  <span className="ml-1 text-gray-500 hover:text-cyan-400 cursor-pointer relative group">
    <HelpCircle size={14} />
    <div className="absolute left-1/2 -top-2 transform -translate-x-1/2 invisible group-hover:visible 
                    w-64 p-2 bg-gray-700 text-xs text-gray-200 rounded-lg shadow-xl z-50">
      {content}
    </div>
  </span>
);

// 指标卡片
const MetricCard = ({ title, value, icon: Icon, unit = '', color = 'text-cyan-400', info }) => (
  <div className="bg-gray-800 p-5 rounded-xl shadow-lg border border-gray-700 transition-all hover:border-cyan-500">
    <div className="flex items-center justify-between">
      <h3 className="text-sm font-medium text-gray-400 flex items-center">
        {title} {info && <InfoTooltip content={info} />}
      </h3>
      <Icon size={20} className={color} />
    </div>
    <div className="mt-4">
      <p className={`text-4xl font-bold ${color}`}>{value}{unit}</p>
    </div>
  </div>
);

// 线性进度条 (用于Global Score)
const ScoreGauge = ({ score }) => {
  const percentage = score * 100;
  let color = 'bg-red-500';
  if (score > 0.4) color = 'bg-yellow-500';
  if (score > 0.7) color = 'bg-green-500';

  return (
    <div className="mt-2 h-2 rounded-full bg-gray-700">
      <div 
        className={`h-full rounded-full transition-all duration-500 ${color}`} 
        style={{ width: `${percentage}%` }}
      ></div>
    </div>
  );
};

// 24HRI 柱状图
const TimeDiversityChart = ({ data }) => (
  <div className="bg-gray-800 p-6 rounded-xl shadow-lg h-full border border-gray-700">
    <h2 className="text-xl font-semibold text-gray-100 mb-4 flex items-center">
      <Clock size={20} className="mr-2 text-cyan-400" /> 24 小时贡献指数 (24HRI) 
      <InfoTooltip content="衡量项目在一天 24 小时内被全球贡献者覆盖的均匀程度。得分越高，项目的全球时区覆盖越好。" />
    </h2>
    <div className="h-80">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 10, right: 10, left: -20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#4B5563" />
          <XAxis 
            dataKey="hour" 
            angle={-45} 
            textAnchor="end"
            height={60}
            stroke="#9CA3AF" 
            tick={{ fill: '#9CA3AF', fontSize: 12 }} 
          />
          <YAxis 
            stroke="#9CA3AF" 
            tick={{ fill: '#9CA3AF', fontSize: 12 }} 
          />
          <Tooltip 
            contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px' }}
            formatter={(value, name, props) => [`提交数: ${value}`, `UTC ${props.payload.hour}`]}
            labelStyle={{ color: '#E5E7EB' }}
          />
          <Bar dataKey="commits" fill="#00C49F" radius={[10, 10, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
    <p className="text-xs text-gray-500 mt-2 text-center">UTC 时间 (小时)</p>
  </div>
);

// 地理多样性面板
const GeoDiversityPanel = ({ data, score }) => (
  <div className="bg-gray-800 p-6 rounded-xl shadow-lg h-full border border-gray-700">
    <h2 className="text-xl font-semibold text-gray-100 mb-4 flex items-center">
      <Globe size={20} className="mr-2 text-cyan-400" /> 地理多样性 (GeoDiversity)
      <InfoTooltip content="基于香农熵计算，评估贡献者地理位置的集中度。得分越高，贡献分布越均匀。" />
    </h2>
    <div className="flex flex-col md:flex-row h-full">
      {/* 甜甜圈图 - 国家贡献占比 */}
      <div className="w-full md:w-1/2 h-72">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={data}
              dataKey="value"
              nameKey="name"
              cx="50%"
              cy="50%"
              innerRadius={60}
              outerRadius={90}
              fill="#8884d8"
              paddingAngle={5}
              labelLine={false}
              label={({ name, percent }) => `${name} (${(percent * 100).toFixed(0)}%)`}
            >
              {data.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px' }}
              formatter={(value, name) => [`提交数: ${value}`, `国家: ${name}`]}
              labelStyle={{ color: '#E5E7EB' }}
            />
            <Legend layout="vertical" align="right" verticalAlign="middle" wrapperStyle={{ paddingLeft: '20px' }} />
          </PieChart>
        </ResponsiveContainer>
      </div>

      {/* 模拟地图和得分 */}
      <div className="w-full md:w-1/2 p-4 flex flex-col items-center justify-center space-y-4">
        <div className="text-center">
          <p className="text-sm text-gray-400">多样性得分 (熵值)</p>
          <p className="text-4xl font-bold text-teal-400">{score.toFixed(2)}</p>
        </div>
        <div className="w-full h-32 bg-gray-700 rounded-lg flex items-center justify-center border border-gray-600">
          <p className="text-gray-400 text-sm italic">地图热力图占位 (需集成 D3.js 或 Three.js)</p>
        </div>
      </div>
    </div>
  </div>
);

// 原始贡献列表
const CommitTable = ({ commits }) => (
  <div className="bg-gray-800 p-6 rounded-xl shadow-lg border border-gray-700">
    <h2 className="text-xl font-semibold text-gray-100 mb-4 flex items-center">
      <Tally3 size={20} className="mr-2 text-cyan-400" /> 原始贡献记录 (最近 40 条)
    </h2>
    <div className="overflow-x-auto h-72">
      <table className="min-w-full divide-y divide-gray-700">
        <thead>
          <tr>
            <th className="px-4 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">时间 (UTC)</th>
            <th className="px-4 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">国家 (ISO-3)</th>
            <th className="px-4 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">贡献者 ID</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-700">
          {commits.slice(0, 40).map((commit, index) => (
            <tr key={index} className="hover:bg-gray-700/50 transition-colors">
              <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-300">
                {new Date(commit.timestamp_unix * 1000).toUTCString().replace("GMT", "")}
              </td>
              <td className="px-4 py-2 whitespace-nowrap text-sm font-mono text-teal-400">{commit.location_iso3}</td>
              <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-400">{commit.contributor_id}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  </div>
);

// LLM Insight Generator Component
const InsightGeneratorCard = ({ data, selectedProject, onGenerate, loading, insightText }) => (
  <div className="bg-gray-800 p-6 rounded-xl shadow-lg border border-gray-700 md:col-span-4">
    <h2 className="text-xl font-semibold text-gray-100 mb-4 flex items-center">
      <Sparkles size={20} className="mr-2 text-yellow-400" /> 全球洞察生成器
      <InfoTooltip content="使用 Gemini LLM 分析当前项目的各项指标，生成专业的全球化报告和改进建议。" />
    </h2>
    
    <button
      onClick={onGenerate}
      disabled={loading || !API_KEY}
      className={`flex items-center justify-center px-4 py-2 text-white font-medium rounded-lg shadow-md transition-all 
                  ${loading 
                    ? 'bg-gray-600 cursor-not-allowed' 
                    : 'bg-cyan-600 hover:bg-cyan-700 hover:shadow-lg'}`}
    >
      {loading ? (
        <>
          <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
          正在生成报告...
        </>
      ) : (
        <>
          <Sparkles size={18} className="mr-2" />
          生成 {selectedProject} 的全球化洞察报告
        </>
      )}
    </button>
    {!API_KEY && (
        <p className="mt-2 text-sm text-red-400">错误：API 密钥为空，请在代码中设置 API_KEY。</p>
    )}

    {insightText && (
      <div className="mt-5 p-4 bg-gray-700/70 rounded-lg border border-gray-600 whitespace-pre-wrap">
        <h3 className="text-lg font-bold text-teal-300 mb-2">分析报告摘要：</h3>
        <p className="text-gray-200">{insightText}</p>
      </div>
    )}
  </div>
);


// --- 主应用组件 ---

const App = () => {
  const [selectedProject, setSelectedProject] = useState(PROJECT_OPTIONS[0]);
  const [loading, setLoading] = useState(true);
  const [insightLoading, setInsightLoading] = useState(false);
  const [insightText, setInsightText] = useState("");
  
  const data = useDataProcessor(selectedProject);
  
  // 模拟加载效果
  useEffect(() => {
    setLoading(true);
    setInsightText(""); // Clear insight on project change
    const timer = setTimeout(() => setLoading(false), 500);
    return () => clearTimeout(timer);
  }, [selectedProject]);
  
  // LLM API Call with Exponential Backoff
  const generateInsight = useCallback(async () => {
    if (!API_KEY) {
      setInsightText("无法生成洞察：API 密钥未设置。");
      return;
    }

    setInsightLoading(true);
    setInsightText("");

    const metricsSummary = `
      项目名称: ${selectedProject}
      总贡献数: ${data.totalCommits}
      独立贡献者数: ${data.totalContributors}
      全球化综合得分: ${data.globalScore.toFixed(2)}
      24HRI 覆盖率: ${data.hriCoverage}%
      地理多样性得分 (熵值): ${data.geoData.diversityScore.toFixed(2)}
      前 5 贡献国家/地区: ${data.topCountries}
    `;

    const systemPrompt = "你是一位专注于开源社区和全球化数据分析的专业顾问。根据提供的项目指标，用简洁、专业的语气，撰写一份包含以下内容的分析报告：1. 评估项目的全球化表现（优劣）。2. 指出报告中显示的明确弱点（例如，时区或地理位置过度集中）。3. 给出至少 2 条具体的行动建议以改善其全球化得分。请使用中文Markdown格式返回报告。";
    
    const userQuery = `请根据以下数据，为项目 ${selectedProject} 生成一份全球化分析报告：\n\n${metricsSummary}`;
    
    const payload = {
        contents: [{ parts: [{ text: userQuery }] }],
        systemInstruction: { parts: [{ text: systemPrompt }] },
    };

    let resultText = "报告生成失败，请检查网络或 API 密钥。";
    const maxRetries = 5;

    for (let i = 0; i < maxRetries; i++) {
        try {
            const url = `${API_URL_GEMINI}?key=${API_KEY}`;
            const response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            
            const result = await response.json();
            const candidate = result.candidates?.[0];

            if (candidate && candidate.content?.parts?.[0]?.text) {
                resultText = candidate.content.parts[0].text;
                break; 
            } else {
                console.error("API 响应结构无效:", result);
            }

        } catch (error) {
            console.error(`API 调用失败 (尝试 ${i + 1}/${maxRetries}):`, error);
            if (i < maxRetries - 1) {
                const delay = Math.pow(2, i) + Math.random();
                console.log(`等待 ${delay.toFixed(2)} 秒后重试...`);
                await new Promise(resolve => setTimeout(resolve, delay * 1000));
            }
        }
    }
    
    setInsightText(resultText);
    setInsightLoading(false);

  }, [selectedProject, data]);
  
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-900">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-cyan-500"></div>
        <p className="ml-4 text-cyan-400">正在加载数据...</p>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-4 sm:p-8 font-sans">
      <header className="mb-8">
        <h1 className="text-3xl font-extrabold text-cyan-400">
          开源项目全球化分析仪表盘
        </h1>
        <p className="text-gray-400 mt-1">评估项目在时区和地域上的贡献多样性。</p>
      </header>

      {/* 项目选择器 */}
      <div className="mb-8 flex flex-col sm:flex-row sm:items-center justify-between bg-gray-800 p-4 rounded-xl shadow-md border border-gray-700">
        <label htmlFor="project-select" className="text-gray-300 mr-4 text-lg">
          选择项目:
        </label>
        <select
          id="project-select"
          value={selectedProject}
          onChange={(e) => setSelectedProject(e.target.value)}
          className="mt-2 sm:mt-0 px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-lg text-white focus:ring-cyan-500 focus:border-cyan-500 transition-colors"
        >
          {PROJECT_OPTIONS.map(proj => (
            <option key={proj} value={proj}>{proj}</option>
          ))}
        </select>
        <div className="sm:hidden mt-3" />
        <p className="text-sm text-gray-500 mt-2 sm:mt-0">总提交数: {data.totalCommits}, 独立贡献者: {data.totalContributors}</p>
      </div>

      {/* 核心指标卡片 & 洞察生成器 */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
        {/* 全局得分 */}
        <div className="md:col-span-2">
          <MetricCard
            title="全球化综合得分 (Global Score)"
            value={data.globalScore.toFixed(2)}
            icon={Globe}
            color={data.globalScore > 0.7 ? 'text-green-400' : data.globalScore > 0.4 ? 'text-yellow-400' : 'text-red-400'}
            info="基于 24HRI 和 GeoDiversity 权重计算的综合评分 (满分 1.0)。"
          />
          <ScoreGauge score={data.globalScore} />
        </div>
        
        {/* 24HRI 得分 */}
        <MetricCard
          title="24HRI 覆盖率"
          value={data.hriCoverage.toFixed(0)}
          unit="%"
          icon={Clock}
          color="text-indigo-400"
          info="有贡献的时区小时数占总小时数 24 的比例。"
        />
        
        {/* OpenRank 模拟 */}
        <MetricCard
          title="OpenRank 趋势"
          value={data.openRank.toFixed(2)}
          icon={TrendingUp}
          color="text-pink-400"
          info="模拟的项目社区活跃度和影响力指标。"
        />

        {/* 洞察生成器卡片，跨越 4 列 */}
        <InsightGeneratorCard 
            data={data}
            selectedProject={selectedProject}
            onGenerate={generateInsight}
            loading={insightLoading}
            insightText={insightText}
        />
      </div>

      {/* 图表区域 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* 左侧：时间多样性 */}
        <div className="h-full">
          <TimeDiversityChart data={data.hriData} />
        </div>

        {/* 右侧：地理多样性 */}
        <div className="h-full">
          <GeoDiversityPanel data={data.geoData.countryData} score={data.geoData.diversityScore} />
        </div>
      </div>
      
      {/* 底部：原始数据 */}
      <CommitTable commits={data.rawCommits} />

    </div>
  );
};

export default App;
