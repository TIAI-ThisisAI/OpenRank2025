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
