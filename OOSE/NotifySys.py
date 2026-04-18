from abc import ABC, abstractmethod

# ==========================================
# 1. THE OBSERVER INTERFACE
# ==========================================
class NotificationObserver(ABC):
    @abstractmethod
    def update(self, message):
        pass

# ==========================================
# 2. CONCRETE OBSERVERS
# ==========================================
class EmailNotifier(NotificationObserver):
    def update(self, message):
        print(f"📧 [Email] Sending alert: {message}")

class SMSNotifier(NotificationObserver):
    def update(self, message):
        print(f"📱 [SMS] Sending text: {message}")

class AppPushNotifier(NotificationObserver):
    def update(self, message):
        print(f"🔔 [Push] App Notification: {message}")

# ==========================================
# 3. THE SUBJECT (Smart Home Monitor)
# ==========================================
class SmartHomeSystem:
    def __init__(self):
        self._observers = []

    def attach(self, observer):
        if observer not in self._observers:
            self._observers.append(observer)

    def detach(self, observer):
        self._observers.remove(observer)

    def notify(self, message):
        for observer in self._observers:
            observer.update(message)

    def trigger_alert(self, sensor_type, value):
        print(f"\n⚠️ SYSTEM ALERT: {sensor_type} reached {value}!")
        self.notify(f"High {sensor_type} detected: {value}")

# ==========================================
# 4. EXECUTION
# ==========================================
if __name__ == "__main__":
    # Initialize System
    home_security = SmartHomeSystem()

    # Create Observers
    user_email = EmailNotifier()
    user_phone = SMSNotifier()
    user_app = AppPushNotifier()

    # User subscribes to specific notifications
    home_security.attach(user_email)
    home_security.attach(user_app)

    # Simulate a Smoke Detector trigger
    home_security.trigger_alert("Smoke", "Level 10")

    # User decides to add SMS later
    home_security.attach(user_phone)
    home_security.trigger_alert("Temperature", "45°C")