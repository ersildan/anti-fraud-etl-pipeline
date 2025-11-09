from consumer_activity import consumer_activity as func_activity
from consumer_clients import consumer_clients as func_clients
from consumer_logins import consumer_logins as func_logins
from consumer_payments import consumer_payments as func_payments
from consumer_transactions import consumer_transactions as func_transactions

def main():
    func_activity()
    func_clients()
    func_logins()
    func_payments()
    func_transactions()

if __name__ == "__main__":
    main()
