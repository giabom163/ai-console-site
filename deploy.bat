@echo off
chcp 65001 >nul
title 博客一键部署

echo ========================================
echo   博客一键部署  (Fuwari - Cloudflare Pages)
echo ========================================
echo.

cd /d "%~dp0"

echo [1/3] 正在构建...
call npm run build
if errorlevel 1 (
    echo.
    echo [错误] 构建失败！请看上面的报错。
    echo 常见原因：文章的 frontmatter 缺 published 字段。
    pause
    exit /b 1
)

echo.
echo [2/3] 构建成功，正在上传到 Cloudflare Pages...
echo.

set CI=true
call npx wrangler pages deploy ./dist --project-name giabom-blog --branch main
if errorlevel 1 (
    echo.
    echo [错误] 上传失败！请看上面的报错。
    pause
    exit /b 1
)

echo.
echo ========================================
echo   部署完成！
echo   网站: https://giabom.online
echo ========================================
echo.
pause
