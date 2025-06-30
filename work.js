const {io} = require("socket.io-client");
const os = require("os");
const cluster = require("cluster");
const axios = require("axios");
const {
  getCPUInformation,
  getMemoryInformation,
  getFrequency,
} = require("./functions");
const {processes} = require("./proccess");

let isSendData = false; // first time socket connect`
let IntervalID = {};

let maxCPUUsageUser = 0;
let maxCPUUsageSystem = 0;
let maxMemoryUsage = 0;
let maxSwapMemoryUsage = 0;
let totalMemory = 0;

let maxProcessCPUUsage = 0;
let maxProcessMemoryUsage = 0;

let disConnectTime = new Date().toUTCString();

// const socket = io("https://staging-socket.wooffer.io");
const socket = io("http://localhost:5500");

let serviceEnvironmentConfiguration = {};
let rateLimitConfig = {}
let globalRateLimitConfig = {}

const RecordData = (usageData = {}) => {
  if (
    !("CPU" in usageData) ||
    !("user" in usageData?.CPU) ||
    !("System" in usageData?.CPU) ||
    !("Memory" in usageData) ||
    !("used" in usageData?.Memory) ||
    !("swapused" in usageData?.Memory) ||
    !("Process" in usageData)
  )
    return;

  totalMemory = +(
    (usageData?.Memory?.total + usageData?.Memory?.swaptotal) /
    (1024 * 1024 * 1024)
  ).toFixed(2);

  const NODE_CPU_LOAD = usageData?.Process?.[process.pid]?.reduce(
    (total, currant) => {
      return [
        total[0] + (currant?.cpu || 0),
        total[1] < currant?.memRss ? currant?.memRss : total[1],
      ];
    },
    [0, 0]
  );

  const numberOfEntries = usageData?.Process?.[process.pid]?.length || 1;
  const averageProcessCpu = NODE_CPU_LOAD[0] / numberOfEntries;
  const averageProcessMem = NODE_CPU_LOAD[1];

  if (
    maxCPUUsageUser + maxCPUUsageSystem <
    usageData?.CPU?.user + usageData?.CPU?.System
  ) {
    maxCPUUsageUser = usageData?.CPU?.user;
    maxCPUUsageSystem = usageData?.CPU?.System;
  }

  if (maxProcessCPUUsage < averageProcessCpu) {
    maxProcessCPUUsage = averageProcessCpu;
  }

  if (maxProcessMemoryUsage < averageProcessMem) {
    maxProcessMemoryUsage = averageProcessMem;
  }

  if (
    maxMemoryUsage + maxSwapMemoryUsage <
    usageData?.Memory?.used + usageData?.Memory?.swapused
  ) {
    maxMemoryUsage = +(usageData?.Memory?.used / (1024 * 1024 * 1024)).toFixed(
      2
    );
    maxSwapMemoryUsage = +(
      usageData?.Memory?.swapused /
      (1024 * 1024 * 1024)
    ).toFixed(2);
  }
};

const isConfigEnabled = (configKey) => {
  return (
    serviceEnvironmentConfiguration &&
    serviceEnvironmentConfiguration.hasOwnProperty(configKey) &&
    serviceEnvironmentConfiguration[configKey]
  );
};

function init(token, serviceToken) {
  const stopMonitoring = () => {
    clearInterval(IntervalID?.id);
    clearInterval(IntervalID?.usageIntervalIndex);
    delete IntervalID?.id;
  };
  const startMonitoring = () => {
    const intervalIndex = setInterval(async () => {
      let usageData = {};

      const CPU_DATA = await getCPUInformation();
      const memoryUsage = await getMemoryInformation();
      const data = await processes(process.ppid);

      const runningProcess = data.list.filter(
        (el) => el.parentPid == process.ppid
      );

      usageData["CPU"] = {
        ...CPU_DATA,
        ...getFrequency(),
        hardware: `${os.cpus()[0].model} (${os.arch()})`,
        core: os.cpus()?.length,
      };
      usageData["Memory"] = {...memoryUsage};
      usageData["Process"] = {[process.pid]: runningProcess};

      if (
        usageData.Process?.[process.pid]?.[0]?.cpu ||
        usageData.Process?.[process.pid]?.[0]?.cpuu ||
        usageData.Process?.[process.pid]?.[0]?.cpus
      )
        RecordData(usageData);

      if (isSendData) {
        socket.emit("usageData", {
          token,
          serviceToken,
          pid: process.pid,
          ppid: process.ppid,
          usageData,
        });
      }
    }, 2500);

    const usageIntervalIndex = setInterval(async () => {
      if (isConfigEnabled("isProcessAndCPUUsageEnabled")) {
        socket.emit("updateUsage", {
          token,
          serviceToken,
          maxCPUUsageUser,
          maxCPUUsageSystem,
          maxMemoryUsage,
          maxSwapMemoryUsage,
          totalMemory,
          maxProcessCPUUsage,
          maxProcessMemoryUsage,
        });
      }
      maxCPUUsageUser = 0;
      maxCPUUsageSystem = 0;
      maxMemoryUsage = 0;
      maxSwapMemoryUsage = 0;
      maxProcessCPUUsage = 0;
      maxProcessMemoryUsage = 0;
      totalMemory = 0;
    }, (+serviceEnvironmentConfiguration?.cpuUsageInterval || 10) * 60 * 1000);

    IntervalID = {
      id: intervalIndex,
      usageIntervalIndex,
    };
  };
  process.on("unhandledRejection", (reason, p) => {
    console.error(reason, "Unhandled Rejection at Promise", p);
    if (isConfigEnabled("isServerActivityLogEnabled")) {
      if (reason?.name !== null && reason?.name !== undefined) {
        socket.emit(
          "error",
          "Name : " +
            reason?.name +
            "\nMessage : " +
            reason?.message +
            "\nstack : " +
            reason?.stack
        );
      } else {
        socket.emit("error", reason.toString());
      }
    }
  });

  process.on("uncaughtException", (err) => {
    console.log(err);
    if (isConfigEnabled("isServerActivityLogEnabled")) {
      if (err?.name !== null && err?.name !== undefined) {
        socket.emit(
          "error",
          "Name : " +
            err.name +
            "\nMessage : " +
            err.message +
            "\nstack : " +
            err.stack
        );
      } else {
        socket.emit("error", err.toString());
      }
    }
  });

  const joinRoomEvent = () => {
    socket.on("updateServiceEnvironmentInformationForPackage", (details) => {
      console.log("🚀 ~ socket.on ~ details:", details)
      serviceEnvironmentConfiguration = {
        serviceEnvironmentId: details.serviceToken || null,
        isAPIEnabled: details.isAPIEnabled || true,
        isServerActivityLogEnabled: details.isServerActivityLogEnabled || true,
        isCustomLogEnabled: details.isCustomLogEnabled || true,
        isProcessAndCPUUsageEnabled: details.isProcessAndCPUUsageEnabled || true,
        cpuUsageInterval: details.cpuUsageInterval || 10,
      }
      globalRateLimitConfig = { ...details.globalRateLimitConfig } || null;
      rateLimitConfig = { ...details.rateLimitConfig } || null
    });

    startMonitoring();
    const kubernetesEnvVars = ["KUBERNETES_SERVICE_HOST", "KUBERNETES_PORT"];
    const isKubernetes = kubernetesEnvVars.every(
      (envVar) => process.env[envVar]
    );

    socket.emit("join-room", {
      token,
      serviceToken,
      pid: process.pid,
      ppid: process.ppid,
      isKubernetes: isKubernetes,
      host: process?.env?.KUBERNETES_SERVICE_HOST,
      port: process?.env?.KUBERNETES_PORT,
      cluster: {
        isWorker: cluster.isWorker,
        isMaster: cluster.isMaster,
      },
    });
  };

  socket.on("connect", joinRoomEvent); // Join the room when connected initially
  socket.on("disconnect", () => {
    disConnectTime = new Date().toUTCString();
    stopMonitoring();
  }); // Join the room when connected initially

  socket.on("startTracing", () => {
    isSendData = true;
  });

  socket.on("stopTracing", () => {
    isSendData = false;
  });

  socket.on("log", (details) => {
    // After running your code, you can check the connection status
    console.log(details);
  });
}

const emitAlert = (type, message = " ") => {
  if (isConfigEnabled("isCustomLogEnabled")) {
    socket.emit("alert", { type, message });
  }
};

const alert = (message) => emitAlert("alert", message);
const success = (message) => emitAlert("success", message);
const fail = (message) => emitAlert("fail", message);

const getRateLimitConfig = (req) => {
  // endpoint config || global config
  const serviceEnvironmentId = serviceEnvironmentConfiguration?.serviceEnvironmentId;
  const key = `${serviceEnvironmentId}:${req?.method}:${req?.url}`;
  const endpointConfig = rateLimitConfig?.[key];
  const conf = {
    ...endpointConfig,
    isRateLimit: globalRateLimitConfig.isRateLimit,
    serviceEnvironmentId,
    ip_key: rateLimitConfig?.ip_key || globalRateLimitConfig.ip_key
  } || globalRateLimitConfig;
  return conf;
};

const isIpBlocked = (ip, serviceEnvironmentId) => {
  return new Promise((resolve) => {
    socket.emit("checkIfIpIsBlocked", { ip, serviceEnvironmentId });
    const handler = (isIpBlocked) => {
      socket.off("isIpBlockedResponse", handler);
      resolve(isIpBlocked);
    };
    socket.on("isIpBlockedResponse", handler);
  });
};

const handleRateLimit = (req) => {
  // get config
  const rateLimitConfig = getRateLimitConfig(req);

  // check if ip is blocked?
  if (!isIpBlocked("some-other-ip", serviceEnvironmentConfiguration.serviceEnvironmentId)) {
    return 'Blocked ip';
  }

  // check if config is present and enabled
  if (rateLimitConfig) {
    return 'no config';
  }

  if (!rateLimitConfig.isRateLimit) {
    return true;
  }
}

const requestMonitoring = (req, res, next) => {
  // TODO: does this goes inside or outside of the isConfigEnabled("isAPIEnabled")

  // handleRateLimit(req)
  if (isConfigEnabled("isAPIEnabled")) {
    // request monitoring
    const requestReceivedTime = new Date();
    socket.emit("requestStart", {
      method: req.method,
      originalUrl: req.originalUrl,
      requestReceivedTime: requestReceivedTime.toUTCString(),
    });

    // Continue to the next middleware or route handler
    res.on("finish", () => {
      const responseSentTime = new Date();
      const timeDifference = responseSentTime - requestReceivedTime;

      // Check the response status
      const responseStatus = res.statusCode;

      socket.emit("responseSent", {
        method: req.method,
        originalUrl: req.originalUrl,
        requestReceivedTime: requestReceivedTime.toUTCString(),
        timeDifference,
        responseStatus,
      });
    });
  }
  next();
};

// Add a request interceptor
axios.interceptors.request.use(
  (config) => {
    const startTime = new Date();
    config.metadata = {startTime: new Date()};
    if (isConfigEnabled("isAPIEnabled")) {
      socket.emit("requestStart", {
        method: config.method,
        type: "ThirdParty",
        originalUrl: config.url,
        requestReceivedTime: startTime.toUTCString(),
      });
    }
    return config;
  },
  (error) => {
    const endTime = new Date();
    const timeDifference = endTime - error.config.metadata.startTime;
    if (isConfigEnabled("isAPIEnabled")) {
      socket.emit("responseSent", {
        method: error.config.method,
        originalUrl: error.config.url,
        requestReceivedTime: error.config.metadata.startTime,
        timeDifference,
        responseStatus: error.response ? error.response.status : "No response",
        errorMessage: error.message,
        type: "ThirdParty",
      });
    }
    return Promise.reject(error);
  }
);

// Add a response interceptor
axios.interceptors.response.use(
  (response) => {
    const endTime = new Date();
    const timeDifference = endTime - response.config.metadata.startTime;
    if (isConfigEnabled("isAPIEnabled")) {
      socket.emit("responseSent", {
        method: response.config.method,
        originalUrl: response.config.url,
        requestReceivedTime: response.config.metadata.startTime,
        timeDifference,
        type: "ThirdParty",
        responseStatus: response.status,
      });
    }

    return response;
  },
  (error) => {
    const endTime = new Date();
    const timeDifference = endTime - error.config.metadata.startTime;
    if (isConfigEnabled("isAPIEnabled")) {
      socket.emit("responseSent", {
        method: error.config.method,
        originalUrl: error.config.url,
        requestReceivedTime: error.config.metadata.startTime,
        timeDifference,
        type: "ThirdParty",
        responseStatus: error.response ? error.response.status : "No response",
        errorMessage: error.message,
      });
    }
    return Promise.reject(error);
  }
);

module.exports = {
  init,
  alert,
  success,
  fail,
  requestMonitoring,
  axios,
};
