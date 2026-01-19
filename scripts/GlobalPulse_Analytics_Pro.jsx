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

