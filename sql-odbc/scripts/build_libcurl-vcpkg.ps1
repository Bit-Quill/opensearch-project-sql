$SRC_DIR = $args[0]

if (!("${SRC_DIR}/packages/curl_x86-windows" | Test-Path))
{
	git clone https://github.com/Microsoft/vcpkg.git $SRC_DIR
	Set-Location $SRC_DIR
	cmd.exe /c bootstrap-vcpkg.bat
	.\vcpkg.exe integrate install
	.\vcpkg.exe install curl[tool]
}
