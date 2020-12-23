# jobmanager

## 项目管理
- 下载到本地后修改
```bash 
git clone git@github.com:DataWorkbench/jobmanager.git
```

- 更新依赖
```bash
go mod tidy
```

- 格式化代码
```bash
make format
```

- 语法检测
```bash
make check
```

- 编译二进制, 输出位置 ./build/bin/sourcemanager
```bash
make compile VERBOSE=yes
```

- 生成 grpc 和 proto buffer 代码
```bash
make generate
```

## 服务
- 查看版本
```bash 
./build/bin/sourcemanager -v
```

- 启动服务
```bash
./build/bin/sourcemanager start -c config/config.yaml
```

