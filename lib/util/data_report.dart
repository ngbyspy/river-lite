// data_report.dart
import 'dart:async';
import 'dart:convert';
import 'package:dio/dio.dart';
import 'package:offer_show/util/storage.dart';

class RequestFeed {
  Future<List<dynamic>> fetch({
    required String behaviorType,
    String? source,
    String? searchQuery,
  }) async {
    // 获取当前用户ID
    String infoTxt = await getStorage(key: "myinfo", initData: "");
    String userId = "0";
    if (infoTxt != "") {
      try {
        userId = jsonDecode(infoTxt)["uid"].toString();
      } catch (e) {
        userId = "0";
      }
    }

    // 构造请求数据
    Map<String, dynamic> requestData = {
      "user_id": userId,
      "behavior_type": behaviorType,
      "source": source ?? "feed",
      "search_query": searchQuery ?? "",
      "timestamp": DateTime.now().millisecondsSinceEpoch,
    };

    try {
      Response response = await Dio().post(
        "http://xusun000.top/api/fetch_data",
        data: requestData,
        options: Options(
          contentType: Headers.jsonContentType,
          sendTimeout: const Duration(seconds: 1),
          receiveTimeout: const Duration(seconds: 1),
        ),
      );

      if (response.statusCode == 200) {
        final data = response.data;
        if (data is List) {
          return data;
        }
      } else {
        print("DEBUG_LOG: 请求失败，状态码: ${response.statusCode}");
      }
      return [];
    } catch (e) {
      if (e is DioException) {
        print("DEBUG_LOG: 获取Feeds失败，错误类型: ${e.type}");
      } else {
        print("DEBUG_LOG: 获取Feeds发生未知错误: $e");
      }
      return [];
    }
  }
}

class DataReport {
  static report({
    required String itemId,
    required String behaviorType,
    String? source,
    String? searchQuery,
    int stayDuration = 0,
    double completionRate = 0.0,
  }) async {
    // 获取当前用户ID
    String infoTxt = await getStorage(key: "myinfo", initData: "");
    String userId = "0";
    if (infoTxt != "") {
      try {
        userId = jsonDecode(infoTxt)["uid"].toString();
      } catch (e) {
        userId = "0";
      }
    }

    // 构造请求数据
    Map<String, dynamic> reportData = {
      "user_id": userId,
      "item_id": itemId,
      "behavior_type": behaviorType,
      "stay_duration": stayDuration, // 之前代码中有toString()，后端逻辑若是Int则保持int
      "source": source ?? "feed",
      "search_query": searchQuery ?? "",
      "completion_rate": completionRate,
      "timestamp": DateTime.now().millisecondsSinceEpoch,
    };

    try {
      Response response = await Dio().post(
        "http://xusun000.top/api/upload_data",
        data: reportData,
        options: Options(contentType: Headers.jsonContentType),
      );
      print("DEBUG_LOG: 埋点上报成功！类型: $behaviorType 服务器返回: ${response.data}");
    } catch (e) {
      if (e is DioException) {
        print("DEBUG_LOG: 埋点上报失败，状态码: ${e.response?.statusCode}");
      } else {
        print("DEBUG_LOG: 埋点上报发生未知错误: $e");
      }
      // 将异常抛出，以便 Scheduler 捕获并重试
      rethrow;
    }
  }
}

class ReportTask {
  final String itemId;
  int retryCount = 0;
  ReportTask(this.itemId);
}

class DataReportScheduler {
  static final List<ReportTask> _queue = [];
  static Timer? _timer;

  // --- 新增：去重与缓存有效期逻辑 ---
  // 记录本会话已成功上报的ID
  static final Set<String> _reportedIds = {};
  // 定时清理缓存的计时器
  static Timer? _historyCleanerTimer;

  static const int MAX_QUEUE_SIZE = 50;
  static const int MAX_RETRIES = 3;
  static const Duration INTERVAL = Duration(milliseconds: 500);
  // 曝光去重有效期：10分钟
  static const Duration HISTORY_VALID_DURATION = Duration(minutes: 1);

  /// 将任务加入队列
  static void addTask(String itemId) {
    // 1. 历史去重检查：如果近期已上报成功，则直接跳过
    if (_reportedIds.contains(itemId)) {
      // print("DEBUG_LOG: [调度器] ID $itemId 近期已上报，拦截重复曝光");
      return;
    }

    // 2. 队列去重：检查是否已在排队中
    if (_queue.any((task) => task.itemId == itemId)) return;

    // 3. 队列容量保护
    if (_queue.length >= MAX_QUEUE_SIZE) {
      print("DEBUG_LOG: [调度器] 队列已满，舍弃任务: $itemId");
      return;
    }

    _queue.add(ReportTask(itemId));
    _startTimer();
    _startHistoryCleaner(); // 确保清理计时器启动
  }

  /// 手动清空历史记录（建议在"下拉刷新"逻辑中调用）
  // static void clearHistory() {
  //   _reportedIds.clear();
  //   print("DEBUG_LOG: [调度器] 已手动清空去重历史");
  // }

  /// 清空队列并停止所有计时器（用于应用退出）
  static void dispose() {
    _queue.clear();
    _reportedIds.clear();
    _timer?.cancel();
    _historyCleanerTimer?.cancel();
    _timer = null;
    _historyCleanerTimer = null;
  }

  // 内部：启动历史记录清理定时器
  static void _startHistoryCleaner() {
    if (_historyCleanerTimer != null && _historyCleanerTimer!.isActive) return;
    _historyCleanerTimer = Timer.periodic(HISTORY_VALID_DURATION, (timer) {
      _reportedIds.clear();
      print("DEBUG_LOG: [调度器] 达到10分钟有效期，自动清空去重历史");
    });
  }

  static void _startTimer() {
    if (_timer != null && _timer!.isActive) return;
    _timer = Timer.periodic(INTERVAL, (timer) async {
      if (_queue.isEmpty) {
        _stopTimer();
        return;
      }

      ReportTask task = _queue.removeAt(0);

      try {
        await DataReport.report(
          itemId: task.itemId,
          behaviorType: "exposure",
        );

        // 上报成功后，加入去重集合
        _reportedIds.add(task.itemId);
      } catch (e) {
        // 异常处理与重试机制
        if (task.retryCount < MAX_RETRIES) {
          task.retryCount++;
          _queue.add(task);
          print(
              "DEBUG_LOG: [调度器] 上报失败，准备第${task.retryCount}次重试: ${task.itemId}");
        } else {
          print("DEBUG_LOG: [调度器] 达到最大重试次数，舍弃任务: ${task.itemId}");
        }
      }
    });
  }

  static void _stopTimer() {
    _timer?.cancel();
    _timer = null;
  }
}
