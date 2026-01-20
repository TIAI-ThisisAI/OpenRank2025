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
