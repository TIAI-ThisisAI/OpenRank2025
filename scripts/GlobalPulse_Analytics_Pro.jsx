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

const apiKey = ""; // 如果你有 API Key，填在这里
const API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent";

const TIME_RANGES = [
  { label: '30D', value: '1M', days: 30 },
  { label: '90D', value: '3M', days: 90 },
  { label: '180D', value: '6M', days: 180 },
  { label: '1Y', value: '1Y', days: 365 },
];

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

/**
 * 模拟数据生成器
 * 逻辑：根据项目类型和时间跨度，生成模拟的 Commit 记录。
 * 包含对时区（根据不同国家代码）和活跃时间段的简单模拟。
 */
const generateMockData = (projectType, days) => {
  const now = Math.floor(Date.now() / 1000);
  const isA = projectType === 'Project-A';
  const count = isA ? days * 12 : days * 6; // Project-A 活跃度更高
  
  const commits = Array.from({ length: count }, () => {
    const ts = now - Math.floor(Math.random() * 86400 * days);
    const hour = new Date(ts * 1000).getUTCHours();
    
    // 根据项目类型分配不同的国家池，模拟地理分布差异
    let pool = isA ? ["USA", "CHN", "DEU", "IND", "JPN"] : ["USA", "GBR", "DEU", "CAN"];
    
    // 简单的时区活跃模拟：根据 UTC 小时数调整活跃国家权重
    if (isA) {
      if (hour >= 2 && hour <= 10) pool = ["CHN", "IND", "JPN", "AUS"]; // 亚太时区
      else if (hour >= 10 && hour <= 18) pool = ["DEU", "GBR", "FRA", "NLD"]; // 欧洲时区
    }
    
    return {
      timestamp_unix: ts,
      location_iso3: pool[Math.floor(Math.random() * pool.length)],
      contributor_id: `u${Math.floor(Math.random() * (isA ? 80 : 30))}`, // 模拟贡献者数量差异
      contributor_name: `Dev-${Math.floor(Math.random() * 9000) + 1000}`
    };
  }).sort((a, b) => a.timestamp_unix - b.timestamp_unix);

  // 生成趋势图的历史数据点
  const historyPoints = days > 90 ? 20 : 10;
  const history = Array.from({ length: historyPoints }, (_, i) => ({
    label: days > 180 ? `${i+1}M` : `W${i+1}`,
    score: 0.5 + Math.random() * 0.4,
    commits: Math.floor(Math.random() * 150) + 40,
    contributors: Math.floor(Math.random() * 50) + 10
  }));

  // 生成雷达图的六维数据
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

/**
 * 核心数据处理 Hook
 * 逻辑：将原始的 Commits 列表聚合成图表所需的各种格式（热力图、饼图、列表等）。
 * 并在 useMemo 中处理，避免不必要的重复计算。
 */
const useDataProcessor = (project, range, filter) => {
  // 1. 获取原始模拟数据
  const rawData = useMemo(() => {
    const config = TIME_RANGES.find(r => r.value === range) || TIME_RANGES[0];
    return generateMockData(project, config.days);
  }, [project, range]);

  // 2. 数据聚合与二次处理
  return useMemo(() => {
    const { commits, history, radarData } = rawData;
    
    const hourly = Array(24).fill(0); 
    const heatmap = Array(7).fill(0).map(() => Array(24).fill(0));
    const geo = {}; 
    const users = {};

    // 遍历所有 Commit 进行分类统计
    commits.forEach(c => {
      const d = new Date(c.timestamp_unix * 1000);
      const h = d.getUTCHours(), day = d.getUTCDay();
      
      hourly[h]++; 
      heatmap[day][h]++; // 构建 周x小时 热力图数据
      geo[c.location_iso3] = (geo[c.location_iso3] || 0) + 1; // 构建地理分布数据
      
      // 用户聚合统计
      if (!users[c.contributor_id]) users[c.contributor_id] = { ...c, count: 0 };
      users[c.contributor_id].count++;
    });

    // --- 模拟数据增强开始 ---
    // 为了让图表看起来更真实，人为增加一些“工作时间”和“周末”的数据模式
    for (let d = 1; d <= 5; d++) { // 周一到周五
      // 增加上午高峰 (9-11点)
      for (let h = 9; h <= 11; h++) {
        const val = Math.floor(Math.random() * 8) + 5;
        heatmap[d][h] += val;
        hourly[h] += val;
      }
      // 增加下午高峰 (14-17点)
      for (let h = 14; h <= 17; h++) {
        const val = Math.floor(Math.random() * 8) + 5;
        heatmap[d][h] += val;
        hourly[h] += val;
      }
      // 偶尔增加加班时间 (20-22点)
      if (Math.random() > 0.5) {
        for (let h = 20; h <= 22; h++) {
            const val = Math.floor(Math.random() * 4) + 1;
            heatmap[d][h] += val;
            hourly[h] += val;
        }
      }
    }
    // 周末低频活动
    [0, 6].forEach(d => {
        if (Math.random() > 0.7) {
            for (let h = 14; h <= 16; h++) {
                const val = Math.floor(Math.random() * 3) + 1;
                heatmap[d][h] += val;
                hourly[h] += val;
            }
        }
    });
    // --- 模拟数据增强结束 ---

    const hriData = hourly.map((c, h) => ({ hour: h.toString().padStart(2, '0'), commits: c }));
    
    // 处理饼图数据：排序并合并"其他"类别
    const countryData = Object.entries(geo).sort((a, b) => b[1] - a[1]).map(([name, value]) => ({ name, value }));
    const pieData = [...countryData.slice(0, 5), ...(countryData.length > 5 ? [{ name: 'Other', value: countryData.slice(5).reduce((s, x) => s + x.value, 0) }] : [])];
    
    // 处理用户筛选
    let filteredUsers = Object.values(users);
    if (filter) filteredUsers = filteredUsers.filter(u => u.location_iso3 === filter);
    
    // 计算综合得分算法（简易版）
    const hriScore = hourly.filter(v => v > 0).length / 24; // 时间覆盖率
    const geoScore = countryData.length > 0 ? Math.min(1, Math.log(countryData.length) / Math.log(8)) : 0; // 地理多样性对数得分

    return {
      hriData, 
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
