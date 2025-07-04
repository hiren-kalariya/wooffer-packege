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
let rateLimitConfigMap = {
  // "<Method>:<url>": config
}
let globalRateLimitConfig = {}

let rateLimitsCount = {
  // remove service env id
  // "<ip>:<Method>:<url>": {
  //   timestamps: [Date.now()]
  //   exceedCount: 0
  // }
};

let blockedIps = [];
let newlyBlockedIps = [];
let ipBlocked = {
  // ip: {
  //   endpoint: {
  //     agent: failedAttemptCount
  //   }
  // }
}

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
    clearInterval(IntervalID?.syncBlockedIpsIndex);
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

    const syncBlockedIpsIndex = setInterval(() => {
      if (newlyBlockedIps.length > 0) {
        // post to db
        socket.emit("syncNewlyBlockedIps", newlyBlockedIps);
        newlyBlockedIps = [];
      }
      // TODO: change time
    }, 20000);

    IntervalID = {
      id: intervalIndex,
      usageIntervalIndex,
      syncBlockedIpsIndex,
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
      console.log("::::: ~ socket.on ~ details:", details)
      serviceEnvironmentConfiguration = {
        serviceEnvironmentId: details.serviceToken || null,
        isAPIEnabled: details.isAPIEnabled || true,
        isServerActivityLogEnabled: details.isServerActivityLogEnabled || true,
        isCustomLogEnabled: details.isCustomLogEnabled || true,
        isProcessAndCPUUsageEnabled: details.isProcessAndCPUUsageEnabled || true,
        cpuUsageInterval: details.cpuUsageInterval || 10,
      }
      globalRateLimitConfig = { ...details.globalRateLimitConfig } || null;

      socket.on("receiveRateLimitConfigs", (data) => {
        rateLimitConfigMap = {
          ...rateLimitConfigMap,
          ...data.rows.reduce((a, c) => {
            a[`${c.method}:${c.endpoint}`] = c;
            return a;
          }, {}),
        };
      });

      socket.on("receiveBlockedIps", (data) => {
        blockedIps = [ ...blockedIps, ...data.rows ];
      })

      socket.emit("requestRateLimitConfigs", { serviceEnvironmentId: serviceToken });
      socket.emit("requestBlockedIps", { serviceEnvironmentId: serviceToken });
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

const getEndpointConfig = (method, url) => {
  // endpoint config || global config
  const serviceEnvironmentId = serviceEnvironmentConfiguration?.serviceEnvironmentId;
  const key = `${method}:${url}`;
  const endpointConfig = rateLimitConfigMap?.[key];
  const conf = {
    ...endpointConfig,
    isRateLimit: globalRateLimitConfig.isRateLimit,
    serviceEnvironmentId,
    ipKey: endpointConfig?.ipKey || globalRateLimitConfig.ipKey
  } || globalRateLimitConfig;
  return conf;
};

const isIpBlocked = (ip) => {
  console.log("::::: Checking if IP is blocked:", ip);
  // console.log("::::: Blocked IPs:", blockedIps);
  // console.log("::::: Newly Blocked IPs:", newlyBlockedIps);
  const isBlocked = [...blockedIps, ...newlyBlockedIps].some((ipBlocked) => ipBlocked.ip === ip);
  console.log("::::: Is IP Blocked:", isBlocked);
  return isBlocked;
}

const resetRateLimitsCount = (Key) => {
  console.log("::::: Resetting rate limits count for key:", Key);
  rateLimitsCount[Key].timestamps = [Date.now()];
  rateLimitsCount[Key].exceedCount = 0;
}

const setRateLimitsCount = (Key) => {
  console.log("::::: Resetting rate limits count for key:", Key);
  rateLimitsCount[Key] = {
    timestamps: [Date.now()],
    exceedCount: 0
  }
}

const handleRateLimit = (req, res) => {
  const endpointConfig = getEndpointConfig(req.method, req.originalUrl);
  console.log("::::: Endpoint Config:", endpointConfig);
  if (!endpointConfig || !endpointConfig.isRateLimit) return true;

  // check if ip is blocked
  const ip = req?.headers[endpointConfig.ipKey];
  console.log("::::: Checking if IP is blocked:", ip);
  if (isIpBlocked(ip)) {
    res.status(403).send({ error: "Ip is blocked" });
    return false;
  }

  const key = `${ip}:${req?.method}:${req?.url}`;
  console.log("::::: Rate Limit Key:", key);
  if (!rateLimitsCount[key]) {
    console.log("::::: No existing rate limit count, resetting.");
    setRateLimitsCount(key);
    console.log("::::: Rate limit counts", rateLimitsCount);
    return true;
  }

  // check if the window is expired
  const timeElapsed = Date.now() - rateLimitsCount[key].timestamps[0];
  console.log("::::: Time elapsed since last request:", timeElapsed);
  if (timeElapsed >= endpointConfig.windowMs) {
    console.log("::::: Window expired, resetting count.");
    resetRateLimitsCount(key);
    return true;
  }

  // check if limit is reached
  const requestCount = rateLimitsCount[key].timestamps.length;
  console.log("::::: Current request count:", requestCount);
  if (requestCount >= endpointConfig.maxRequests) {
    // increment exceed count
    rateLimitsCount[key].exceedCount += 1;
    console.log("::::: Exceed count incremented:", rateLimitsCount[key].exceedCount);

    // if IP blocking limit is reached then block the IP
    if (endpointConfig?.isBlockAfterFault && rateLimitsCount[key].exceedCount >= endpointConfig.faultAllowLimit) {
      newlyBlockedIps.push({
        serviceEnvironmentId: serviceEnvironmentConfiguration?.serviceEnvironmentId,
        ip,
        blockTime: new Date().toUTCString()
      });
      console.log("::::: IP blocked due to exceeding fault limit:", ip);

      res.status(429).send({ message: `${endpointConfig.rateLimitExceedErrMsg}, Your IP has been blocked.` });
      return false;
    }

    console.log("::::: Rate limit exceeded, sending response.");
    res.status(429).send({ message: endpointConfig.rateLimitExceedErrMsg });
    return false;
  }

  console.log("::::: Request within limit, resetting count.");
  // resetRateLimitsCount(key);
  rateLimitsCount[key].timestamps.push(Date.now());
  console.log("::::: Rate limit counts", rateLimitsCount);
  return true;
}

const requestMonitoring = (req, res, next) => {
  console.log("::::: Request Monitoring");
  // TODO: does this goes inside or outside of the isConfigEnabled("isAPIEnabled")
  if (!handleRateLimit(req, res)) {
    console.log('handleRateLimit false');
    return
  };
  
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
