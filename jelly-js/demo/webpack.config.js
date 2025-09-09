const CopyPlugin = require("copy-webpack-plugin");
const path = require("path");

module.exports = {
    entry: "./index.js",
    output: {
        path: path.resolve(__dirname, "dist"),
        filename: "index.js",
    },
    module: {
        rules: [
            {
                test: /\.css$/i,
                use: ["style-loader", "css-loader"],
            },
        ],
    },
    mode: "development",
    experiments: {
        asyncWebAssembly: true,
    },
    plugins: [
        new CopyPlugin({
            patterns: [
                { from: "index.html" },
            ],
        }),
    ],
};
