from backup_suite import BackupSuite
import time


def main():
    t0 = time.perf_counter()
    suite = BackupSuite(config='config.yml')
    suite.single_backup()
    t1 = time.perf_counter()
    print('\n')
    print(f'TIME: {t1-t0}')


if __name__ == '__main__':
    main()