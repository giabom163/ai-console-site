import type {
	ExpressiveCodeConfig,
	LicenseConfig,
	NavBarConfig,
	ProfileConfig,
	SiteConfig,
} from "./types/config";
import { LinkPreset } from "./types/config";

export const siteConfig: SiteConfig = {
	title: "giabom 的博客",
	subtitle: "技术折腾 · 项目记录 · 收藏夹月报",
	lang: "zh_CN",
	themeColor: {
		hue: 210, // 蓝色系。想换颜色改这里：红 0 / 橙 30 / 黄 55 / 绿 140 / 青 200 / 蓝 250 / 紫 290 / 粉 345
		fixed: false,
	},
	banner: {
		enable: false,
		src: "assets/images/demo-banner.png",
		position: "center",
		credit: {
			enable: false,
			text: "",
			url: "",
		},
	},
	toc: {
		enable: true,
		depth: 2,
	},
	favicon: [],
};

export const navBarConfig: NavBarConfig = {
	links: [
		{
			name: "核查台",
			url: "/",
		},
		{
			name: "博客",
			url: "/blog/",
		},
		{
			name: "归档",
			url: "/blog/archive/",
		},
		LinkPreset.About,
		{
			name: "GitHub",
			url: "https://github.com/giabom163",
			external: true,
		},
		{
			name: "B站",
			url: "https://space.bilibili.com/642111945",
			external: true,
		},
	],
};

export const profileConfig: ProfileConfig = {
	avatar: "assets/images/demo-avatar.png",
	name: "giabom",
	bio: "喜欢折腾技术，也爱打游戏。写自动化脚本、做 Ren'Py mod、研究部署方案。",
	links: [
		{
			name: "GitHub",
			icon: "fa6-brands:github",
			url: "https://github.com/giabom163",
		},
		{
			name: "Bilibili",
			icon: "fa6-brands:bilibili",
			url: "https://space.bilibili.com/642111945",
		},
	],
};

export const licenseConfig: LicenseConfig = {
	enable: true,
	name: "CC BY-NC-SA 4.0",
	url: "https://creativecommons.org/licenses/by-nc-sa/4.0/",
};

export const expressiveCodeConfig: ExpressiveCodeConfig = {
	theme: "github-dark",
};
