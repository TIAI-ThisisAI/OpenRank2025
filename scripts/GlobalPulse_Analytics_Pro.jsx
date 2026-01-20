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
